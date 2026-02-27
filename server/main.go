package main

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/bytedance/sonic"
	"github.com/gobwas/ws"
	"github.com/panjf2000/gnet/v2"
	"go.uber.org/zap"
)

const (
	maxMessageSize = 512 * 1024
	numShards      = 64               // harus power of 2 untuk bitwise AND
	batchWindow    = time.Millisecond // kumpulkan pesan selama 1ms sebelum flush
	batchMaxSize   = 32               // max pesan per batch sebelum force flush
)

// ─── Object Pools ─────────────────────────────────────────────────────────────
// Reuse buffer objects — zero heap alloc per pesan

var framePool = sync.Pool{
	New: func() any { return &bytes.Buffer{} },
}

var msgPool = sync.Pool{
	New: func() any { return &Message{} },
}

var payloadPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)
		return &b
	},
}

// ─── Message ─────────────────────────────────────────────────────────────────

type Message struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Msg    string `json:"msg"`
	SentAt int64  `json:"sent_at"`
}

// ─── Sharded Hub ─────────────────────────────────────────────────────────────
// 64 shard independen — setiap shard punya lock sendiri
// Dengan 10K client, rata-rata tiap shard isi ~156 client
// Contention turun ~64x dibanding single lock

type hubShard struct {
	mu      sync.RWMutex
	clients map[string]gnet.Conn
	_       [56]byte // padding — cegah false sharing antar CPU cache line (64 byte)
}

type ShardedHub struct {
	shards [numShards]hubShard
	logger *zap.Logger
}

func NewShardedHub(logger *zap.Logger) *ShardedHub {
	h := &ShardedHub{logger: logger}
	for i := range h.shards {
		h.shards[i].clients = make(map[string]gnet.Conn, 256)
	}
	return h
}

// shardIdx pakai FNV hash + bitwise AND (lebih cepat dari modulo)
func (h *ShardedHub) shardIdx(name string) uint32 {
	hh := fnv.New32a()
	hh.Write([]byte(name))
	return hh.Sum32() & (numShards - 1)
}

func (h *ShardedHub) register(name string, conn gnet.Conn) {
	s := &h.shards[h.shardIdx(name)]
	s.mu.Lock()
	s.clients[name] = conn
	s.mu.Unlock()
	h.logger.Info("connected", zap.String("name", name))
}

func (h *ShardedHub) unregister(name string) {
	s := &h.shards[h.shardIdx(name)]
	s.mu.Lock()
	delete(s.clients, name)
	s.mu.Unlock()
	h.logger.Info("disconnected", zap.String("name", name))
}

func (h *ShardedHub) route(msg *Message) {
	s := &h.shards[h.shardIdx(msg.To)]
	s.mu.RLock()
	targetConn, ok := s.clients[msg.To]
	s.mu.RUnlock()

	if !ok {
		return
	}

	data, err := sonic.Marshal(msg)
	if err != nil {
		h.logger.Error("marshal error", zap.Error(err))
		return
	}

	// Build WS frame dari pool buffer
	frameBuf := framePool.Get().(*bytes.Buffer)
	frameBuf.Reset()
	ws.WriteFrame(frameBuf, ws.NewTextFrame(data))

	// Copy karena buf dikembalikan ke pool
	out := make([]byte, frameBuf.Len())
	copy(out, frameBuf.Bytes())
	framePool.Put(frameBuf)

	// AsyncWrite — non-blocking, gnet handle queue-nya
	targetConn.AsyncWrite(out, nil)
}

// ─── ConnContext ──────────────────────────────────────────────────────────────

type ConnContext struct {
	upgraded bool
	name     string

	// Write batching — kumpulkan frame sebelum flush ke kernel
	batchMu    sync.Mutex
	batchBuf   *bytes.Buffer
	batchCount int
	batchTimer *time.Timer
	conn       gnet.Conn
}

func newConnContext(conn gnet.Conn) *ConnContext {
	ctx := &ConnContext{
		batchBuf: framePool.Get().(*bytes.Buffer),
		conn:     conn,
	}
	ctx.batchBuf.Reset()
	return ctx
}

// flushBatch kirim semua pesan yang di-buffer sekaligus (1 syscall)
func (ctx *ConnContext) flushBatch() {
	ctx.batchMu.Lock()
	defer ctx.batchMu.Unlock()

	if ctx.batchBuf.Len() == 0 {
		return
	}

	out := make([]byte, ctx.batchBuf.Len())
	copy(out, ctx.batchBuf.Bytes())
	ctx.batchBuf.Reset()
	ctx.batchCount = 0
	if ctx.batchTimer != nil {
		ctx.batchTimer.Stop()
		ctx.batchTimer = nil
	}

	ctx.conn.AsyncWrite(out, nil)
}

// writeFrame tambah frame ke batch, flush kalau sudah penuh atau setelah 1ms
func (ctx *ConnContext) writeFrame(frame ws.Frame) {
	ctx.batchMu.Lock()

	ws.WriteFrame(ctx.batchBuf, frame)
	ctx.batchCount++

	shouldFlush := ctx.batchCount >= batchMaxSize

	// Set timer flush kalau belum ada
	if ctx.batchTimer == nil && !shouldFlush {
		ctx.batchTimer = time.AfterFunc(batchWindow, func() {
			ctx.flushBatch()
		})
	}

	ctx.batchMu.Unlock()

	if shouldFlush {
		ctx.flushBatch()
	}
}

// ─── connRW ──────────────────────────────────────────────────────────────────

type connRW struct{ c gnet.Conn }

func (rw *connRW) Read(b []byte) (int, error)  { return rw.c.Read(b) }
func (rw *connRW) Write(b []byte) (int, error) { return rw.c.Write(b) }

// ─── WSServer ─────────────────────────────────────────────────────────────────

type WSServer struct {
	gnet.BuiltinEventEngine
	hub    *ShardedHub
	eng    gnet.Engine
	logger *zap.Logger
}

func (s *WSServer) OnBoot(eng gnet.Engine) gnet.Action {
	s.eng = eng
	s.logger.Info("server booted",
		zap.Int("shards", numShards),
		zap.Duration("batch_window", batchWindow),
		zap.Int("batch_max", batchMaxSize),
	)
	return gnet.None
}

func (s *WSServer) OnOpen(c gnet.Conn) ([]byte, gnet.Action) {
	c.SetContext(newConnContext(c))
	return nil, gnet.None
}

func (s *WSServer) OnClose(c gnet.Conn, _ error) gnet.Action {
	ctx, ok := c.Context().(*ConnContext)
	if !ok || ctx == nil {
		return gnet.None
	}
	if ctx.upgraded && ctx.name != "" {
		s.hub.unregister(ctx.name)
	}
	if ctx.batchBuf != nil {
		framePool.Put(ctx.batchBuf)
	}
	if ctx.batchTimer != nil {
		ctx.batchTimer.Stop()
	}
	return gnet.None
}

func (s *WSServer) OnTraffic(c gnet.Conn) gnet.Action {
	ctx := c.Context().(*ConnContext)

	if !ctx.upgraded {
		action := s.handleUpgrade(c, ctx)
		if action != gnet.None || !ctx.upgraded {
			return action
		}
	}

	return s.handleFrames(c, ctx)
}

func (s *WSServer) handleUpgrade(c gnet.Conn, ctx *ConnContext) gnet.Action {
	var clientName string

	upgrader := ws.Upgrader{
		OnRequest: func(uri []byte) error {
			u, err := url.ParseRequestURI(string(uri))
			if err != nil {
				return err
			}
			name := u.Query().Get("name")
			if name == "" {
				return fmt.Errorf("missing name param")
			}
			clientName = name
			return nil
		},
	}

	if _, err := upgrader.Upgrade(&connRW{c}); err != nil {
		s.logger.Warn("upgrade failed", zap.Error(err))
		return gnet.Close
	}

	ctx.upgraded = true
	ctx.name = clientName
	s.hub.register(clientName, c)
	return gnet.None
}

func (s *WSServer) handleFrames(c gnet.Conn, ctx *ConnContext) gnet.Action {
	for {
		n := c.InboundBuffered()
		if n == 0 {
			break
		}

		buf, _ := c.Peek(n)
		br := bytes.NewReader(buf)

		frame, err := ws.ReadFrame(br)
		if err != nil {
			break // partial frame, tunggu data berikutnya
		}

		c.Discard(n - br.Len())

		if frame.Header.Masked {
			ws.Cipher(frame.Payload, frame.Header.Mask, 0)
		}

		switch frame.Header.OpCode {
		case ws.OpText, ws.OpBinary:
			if int64(len(frame.Payload)) > maxMessageSize {
				s.logger.Warn("message too large", zap.String("client", ctx.name))
				return gnet.Close
			}
			s.handleMessage(ctx, frame.Payload)

		case ws.OpPing:
			// Masuk ke batch, bukan langsung write
			ctx.writeFrame(ws.NewPongFrame(frame.Payload))

		case ws.OpClose:
			ctx.writeFrame(ws.Frame{
				Header: ws.Header{OpCode: ws.OpClose, Fin: true},
			})
			ctx.flushBatch()
			return gnet.Close
		}
	}

	return gnet.None
}

func (s *WSServer) handleMessage(ctx *ConnContext, payload []byte) {
	// Ambil Message dari pool — zero alloc
	msg := msgPool.Get().(*Message)
	*msg = Message{} // reset field sebelum pakai

	if err := sonic.Unmarshal(payload, msg); err != nil {
		s.logger.Warn("invalid message",
			zap.String("client", ctx.name),
			zap.Error(err),
		)
		msgPool.Put(msg)
		return
	}

	msg.From = ctx.name
	s.hub.route(msg)
	msgPool.Put(msg)
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	hub := NewShardedHub(logger)
	server := &WSServer{hub: hub, logger: logger}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		logger.Info("shutting down...")
		server.eng.Stop(context.Background())
	}()

	logger.Info("server listening on :3000")
	if err := gnet.Run(
		server,
		"tcp://:3000",
		gnet.WithMulticore(true),             // 1 event loop per CPU core
		gnet.WithReusePort(true),             // kernel load balance antar event loop
		gnet.WithTCPNoDelay(gnet.TCPNoDelay), // disable Nagle — kirim packet langsung
		gnet.WithReadBufferCap(4096),         // read buffer per conn
		gnet.WithWriteBufferCap(4096),        // write buffer per conn
	); err != nil {
		logger.Fatal("gnet error", zap.Error(err))
	}
}
