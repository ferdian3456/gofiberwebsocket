package main

import (
	"fmt"
	"log"
	"time"

	"github.com/fasthttp/websocket"
)

func main() {
	conn, _, err := websocket.DefaultDialer.Dial("ws://localhost:3000/ws", nil)
	if err != nil {
		log.Fatal("error connect:", err)
	}
	defer conn.Close()

	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				fmt.Println("error read:", err)
				return
			}
			fmt.Println("recv:", string(msg))
		}
	}()

	for i := 0; i < 5; i++ {
		msg := fmt.Sprintf("hello ke-%d", i+1)
		err := conn.WriteMessage(websocket.TextMessage, []byte(msg))
		if err != nil {
			fmt.Println("error send:", err)
			break
		}
		fmt.Println("sent:", msg)
		time.Sleep(1 * time.Second)
	}

	err = conn.WriteMessage(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "done"))
	if err != nil {
		fmt.Println("error close ws conn: ", err)
	}

	time.Sleep(500 * time.Millisecond)
}
