package main

import (
	"fmt"

	"github.com/gofiber/contrib/v3/websocket"
	"github.com/gofiber/fiber/v3"
)

func main() {
	app := fiber.New()

	app.Use("/ws", websocket.New(func(c *websocket.Conn) {
		fmt.Println("got here")
		for {
			mt, msg, err := c.ReadMessage()
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					fmt.Println("client disconnect gracefully")
				} else {
					fmt.Println("error read message: ", err)
				}
				break
			}
			fmt.Println("recv: ", string(msg))
			err = c.WriteMessage(mt, msg)
			if err != nil {
				fmt.Println("error write message: ", err)
				break
			}
		}
	}))

	app.Listen(":3000")
}
