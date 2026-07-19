package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type Clientmanager struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
}

type Client struct {
	id     string
	socket *websocket.Conn
	send   chan []byte
}

type Message struct {
	Sender    string `json:"sender,omitempty"`
	Recipient string `json:"recipient,omitempty"`
	Content   string `json:"content,omitempty"`
}

var manager = Clientmanager{
	broadcast:  make(chan []byte),
	register:   make(chan *Client),
	unregister: make(chan *Client),
	clients:    make(map[*Client]bool),
}

func (manager *Clientmanager) start() {
	select {
	case conn := <-manager.register:
		manager.clients[conn] = true
		//fmt.Println("/A new socket has connected.")
		jsonMes, _ := json.Marshal(&Message{Content: "/A new socket has connected."})
		manager.send(jsonMes, conn)
	case conn := <-manager.unregister:
		if _, ok := manager.clients[conn]; ok {
			close(conn.send)
			delete(manager.clients, conn)
			//fmt.Println("/A socket has disconnected.")
			jsonMes, _ := json.Marshal(&Message{Content: "/A socket has disconnected."})
			manager.send(jsonMes, conn)
		}
	case message := <-manager.broadcast:
		for conn := range manager.clients {
			select {
			case conn.send <- message:
			default:
				close(conn.send)
				delete(manager.clients, conn)

			}
		}
	}
}

func (manager *Clientmanager) send(message []byte, ignore *Client) {
	for conn := range manager.clients {
		fmt.Println(string(message))
		if conn != ignore {
			conn.send <- message
		}
	}
}

func (c *Client) read() {
	defer func() {
		manager.unregister <- c
		c.socket.Close()
	}()
	for {
		_, message, err := c.socket.ReadMessage()
		if err != nil {
			manager.unregister <- c
			c.socket.Close()
		}
		jsonMes, _ := json.Marshal(&Message{Sender: c.id, Content: string(message)})
		//fmt.Println(c.id, ":", string(message))
		manager.broadcast <- jsonMes
	}
}

func (c *Client) write() {
	defer func() {
		c.socket.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				c.socket.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			c.socket.WriteMessage(websocket.TextMessage, message)
		}
	}
}

func wsPage(res http.ResponseWriter, req *http.Request) {
	conn, err := (&websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}).Upgrade(res, req, nil)
	if err != nil {
		http.NotFound(res, req)
		return
	}
	newUUID := uuid.New()
	client := &Client{id: newUUID.String(), socket: conn, send: make(chan []byte)}
	manager.register <- client
	go client.read()
	go client.write()
}

func main() {
	fmt.Println("Just chatting...")
	go manager.start()
	http.HandleFunc("/ws", wsPage)
	http.ListenAndServe(":12345", nil)
}
