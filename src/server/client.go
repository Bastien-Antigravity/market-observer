package server

import (
	"time"

	"github.com/gorilla/websocket"
)

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

const (
	writeWait      = 2 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 1024 * 1024 // 1MB for larger JSON messages
)

// -----------------------------------------------------------------------------
// Client Structure
// -----------------------------------------------------------------------------

type Client struct {
	hub  *FastAPIServer
	conn *websocket.Conn
	send chan interface{}
}

// -----------------------------------------------------------------------------
// readPump - handles incoming messages from client
// Act as a Watchdog for the connection
// -----------------------------------------------------------------------------

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
		c.hub.Logger.Info("Client disconnected")
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.hub.Logger.Info("WebSocket error: %v", err)
			}
			break
		}
		// Handle the message (subscribe commands)
		c.hub.HandleClientMessage(c, message)
	}
}

// -----------------------------------------------------------------------------
// writePump - sends messages to client
// -----------------------------------------------------------------------------

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Hub closed the channel
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			// Write JSON message
			if err := c.conn.WriteJSON(message); err != nil {
				c.hub.Logger.Info("Write error: %v", err)
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
