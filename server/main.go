package main

import (
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/bytedance/sonic"
	gws "github.com/fasthttp/websocket"
	"github.com/gofiber/contrib/v3/websocket"
	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512 * 1024
	sendBufSize    = 1024
)

type Message struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Msg    string `json:"msg"`
	SentAt int64  `json:"sent_at"`
}

// ─── Hub ─────────────────────────────────────────────────────────────────────

type Hub struct {
	clients sync.Map
	logger  *zap.Logger
}

func NewHub(logger *zap.Logger) *Hub {
	return &Hub{logger: logger}
}

func (h *Hub) register(c *Client) {
	h.clients.Store(c.name, c)
	h.logger.Info("client connected", zap.String("name", c.name))
}

func (h *Hub) unregister(c *Client) {
	h.clients.Delete(c.name)
	h.logger.Info("client disconnected", zap.String("name", c.name))
}

func (h *Hub) route(msg Message) {
	val, ok := h.clients.Load(msg.To)
	if !ok {
		return
	}
	target := val.(*Client)

	data, err := sonic.Marshal(msg)
	if err != nil {
		h.logger.Error("marshal error", zap.Error(err))
		return
	}

	select {
	case target.send <- data:
	default:
		h.logger.Warn("send buffer full, dropping", zap.String("target", msg.To))
	}
}

// ─── Client ──────────────────────────────────────────────────────────────────

type Client struct {
	hub    *Hub
	name   string
	conn   *websocket.Conn
	send   chan []byte
	logger *zap.Logger
}

func NewClient(hub *Hub, name string, conn *websocket.Conn, logger *zap.Logger) *Client {
	return &Client{
		hub:    hub,
		name:   name,
		conn:   conn,
		send:   make(chan []byte, sendBufSize),
		logger: logger.With(zap.String("client", name)),
	}
}

func (c *Client) ReadPump() {
	defer func() {
		c.hub.unregister(c)
		close(c.send)
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if gws.IsCloseError(err, gws.CloseNormalClosure, gws.CloseGoingAway) {
				c.logger.Info("disconnected gracefully")
			} else {
				c.logger.Error("read error", zap.Error(err))
			}
			break
		}

		var msg Message
		if err := sonic.Unmarshal(data, &msg); err != nil {
			c.logger.Warn("invalid message format", zap.Error(err))
			continue
		}

		msg.From = c.name
		c.hub.route(msg)
	}
}

func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(gws.CloseMessage, gws.FormatCloseMessage(gws.CloseNormalClosure, ""))
				return
			}
			if err := c.conn.WriteMessage(gws.TextMessage, msg); err != nil {
				c.logger.Error("write error", zap.Error(err))
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(gws.PingMessage, nil); err != nil {
				c.logger.Error("ping error", zap.Error(err))
				return
			}
		}
	}
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	logger.Info("server info", zap.Int("gomaxprocs", runtime.GOMAXPROCS(0)))

	hub := NewHub(logger)

	app := fiber.New(fiber.Config{
		Concurrency:  1024 * 1024,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	})

	app.Use("/ws", websocket.New(func(c *websocket.Conn) {
		name := c.Query("name")
		if name == "" {
			logger.Warn("rejected connection: missing name param")
			c.Close()
			return
		}

		client := NewClient(hub, name, c, logger)
		hub.register(client)

		go client.WritePump()
		client.ReadPump()
	}))

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		logger.Info("shutting down server...")
		app.ShutdownWithTimeout(10 * time.Second)
	}()

	logger.Info("server listening on :3000")
	if err := app.Listen(":3000"); err != nil {
		logger.Fatal("server error", zap.Error(err))
	}
}
