// websocket.js

class WebSocketClient {
    constructor(url, onMessageCallback) {
        this.url = url;
        this.onMessageCallback = onMessageCallback;
        this.connect();
    }

    connect() {
        this.socket = new WebSocket(this.url);

        this.socket.onopen = () => {
            //updateConnectionStatus("Connected");
            //connection[++cpt]={"Date": formatDateUTC(new Date()), "Message": "WebSocket started"};
        };

        this.socket.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.onMessageCallback(data);
        };

        this.socket.onclose = (event) => {
            //updateConnectionStatus("Connection closed");
            //connection[++cpt]={"Date": formatDateUTC(new Date()), "Message": "Connection closed"};
            this.reconnect();
        };

        this.socket.onerror = (error) => {
            //updateConnectionStatus(error);
            //connection[++cpt]={"Date": formatDateUTC(new Date()), "Message": error};
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
