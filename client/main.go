package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/fasthttp/websocket"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
)

type Message struct {
	From string `json:"from"`
	To   string `json:"to"`
	Msg  string `json:"msg"`
}

func main() {
	name := flag.String("name", "", "nama client (contoh: clientA)")
	to := flag.String("to", "", "tujuan pesan (contoh: clientB), bisa multiple: clientB,clientC")
	flag.Parse()

	if *name == "" || *to == "" {
		log.Fatal("usage: go run client.go -name=clientA -to=clientB")
	}

	targets := strings.Split(*to, ",") // support multiple target

	url := fmt.Sprintf("ws://localhost:3000/ws?name=%s", *name)
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Fatal("connect error:", err)
	}
	defer conn.Close()

	fmt.Printf("[%s] connected | targets: %v\n", *name, targets)

	// Handle pong
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	done := make(chan struct{})

	// Goroutine: baca pesan masuk
	go func() {
		defer close(done)
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					fmt.Printf("[%s] server closed connection\n", *name)
				} else {
					fmt.Printf("[%s] read error: %v\n", *name, err)
				}
				return
			}
			var msg Message
			if err := json.Unmarshal(data, &msg); err != nil {
				fmt.Printf("[%s] invalid msg: %v\n", *name, err)
				continue
			}
			fmt.Printf("\n[%s] pesan dari '%s': %s\n> ", *name, msg.From, msg.Msg)
		}
	}()

	// Goroutine: ping periodik
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	go func() {
		for range ticker.C {
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}()

	// Baca input dari stdin dan kirim ke semua target
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			fmt.Print("> ")
			continue
		}
		if text == "/quit" {
			break
		}

		for _, target := range targets {
			msg := Message{To: strings.TrimSpace(target), Msg: text}
			data, _ := json.Marshal(msg)

			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				fmt.Printf("[%s] send error: %v\n", *name, err)
				return
			}
		}
		fmt.Print("> ")
	}

	// Graceful close
	conn.SetWriteDeadline(time.Now().Add(writeWait))
	conn.WriteMessage(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "done"),
	)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}
}
