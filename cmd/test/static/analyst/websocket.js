// websocket.js

class WebSocketClient {
    constructor({url, onMessageCallback, onOpenCallback=null, onCloseCallback=null, onErrorCallback=null}) {
        this.url = url;
        this.onMessageCallback = onMessageCallback;
        this.onOpenCallback = onOpenCallback;
        this.onCloseCallback = onCloseCallback;
        this.onErrorCallback = onErrorCallback;
        this.connect();
    }

    connect() {
        this.socket = new WebSocket(this.url);

        this.socket.onopen = () => {
            if (this.onOpenCallback) {this.onOpenCallback();}
        };

        this.socket.onmessage = (event) => {
            let data = JSON.parse(event.data);
            this.onMessageCallback(data);
        };

        this.socket.onclose = (event) => {
            if (this.onCloseCallback) {this.onCloseCallback(event);}
            this.reconnect();
        };

        this.socket.onerror = (error) => {
            if (this.onErrorCallback) {this.onErrorCallback(error);}
        };
    }

    reconnect() {
        console.log('Attempting to reconnect in 5 seconds...');
        setTimeout(() => {
            this.connect();
        }, 5000);
    }

    sendMessage(message) {
        if (this.socket.readyState === WebSocket.OPEN) {
            this.socket.send(JSON.stringify(message));
        }
    }
}

export default WebSocketClient;
