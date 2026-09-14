package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	url := os.Getenv("TAB_WS_URL")
	token := os.Getenv("TAB_ACCESS_TOKEN")
	if url == "" || token == "" {
		fmt.Fprintln(os.Stderr, "TAB_WS_URL and TAB_ACCESS_TOKEN are required")
		os.Exit(2)
	}
	header := http.Header{}
	header.Set("Authorization", "Bearer "+token)
	conn, response, err := websocket.DefaultDialer.Dial(url, header)
	if err != nil {
		if response != nil {
			fmt.Fprintf(os.Stderr, "WebSocket status: %s\n", response.Status)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_, message, err := conn.ReadMessage()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(message))
}
