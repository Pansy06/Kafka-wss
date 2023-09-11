// Import required libraries
import https from 'https';
import fs from 'fs';
import { Kafka, logLevel } from 'kafkajs';
import * as WebSocket from 'ws';


// Specify the passphrase for your SSL private key
const privateKeyPassphrase = 'your_passphrase_here'; // Replace with your passphrase

// Create an HTTPS server with the passphrase
const server = https.createServer({
  cert: fs.readFileSync('your_ssl_certificate.crt'), // Replace with your SSL certificate file path
  key: fs.readFileSync('your_ssl_private_key.key'),   // Replace with your SSL private key file path
  passphrase: privateKeyPassphrase, // Add the passphrase here
});

// Create a WebSocket server by attaching it to the HTTPS server
const wss = new WebSocket.WebSocketServer({ server });

// Create a Kafka client
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['10.240.208.54:9092'], // Replace with your Kafka broker(s) URL
});

// Create a Kafka consumer
const consumer = kafka.consumer({ groupId: '0' });

// Function to send messages to WebSocket clients
const sendMessageToClients = (message) => {
  console.log('sendMessageToClients works 1');
  wss.clients.forEach((client) => {
    if (client.readyState == WebSocket.OPEN && message != undefined) {
      console.log('sendMessageToClients works 2');
      client.send(message.value.toString());
    }
  });
};

// Broadcast to all.
wss.broadcast = function broadcast(data) {
  console.log('broadcast works 1');
  wss.clients.forEach(function each(client) {
    if ( client.readyState == WebSocket.OPEN && data != undefined ) 
      console.log('broadcast works 2');
      client.send(data);
  });
};

// WebSocket server event handling
wss.on('connection', (ws) => {
  console.log('Client connected');
  
  //Periodically send messages to the client
  // setInterval(() => {
  //   wss.clients.forEach(function each(client) {
  //     if ( client.readyState == WebSocket.OPEN && data != undefined ) 
  //       console.log('peridically broadcast message');
  //       client.send("Hello World");
  //   });

  // }, 1000); // Adjust the interval (in milliseconds) to your desired frequency

  // //Periodically send messages to the client
  // setInterval(() => {
  //   wss.broadcast("Hello World");

  // }, 1000); // Adjust the interval (in milliseconds) to your desired frequency

  // Handle incoming messages from the client
  // ws.on('message', (message) => {
  //   console.log(`Received: ${message}`);
    
  //   // Send a response back to the client
  //   ws.send(`Server received: ${message}`);
  // });

  ws.on('message', function incoming(data) {
    console.log(`Received: ${data.toString()}`);
    // Broadcast to everyone else.
    wss.broadcast(data.toString());
  });


  // Handle WebSocket closure
  ws.on('close', () => {
    console.log('Client disconnected');
  });

});

// Start the Kafka consumer
const runConsumer = async () => {
  await consumer.connect('Kafka consumer start');
  await consumer.subscribe({ topic: 'status_topic', fromBeginning: true }); // Replace with your Kafka topic
  console.log('received kafaka message 1');
  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      console.log('received kafaka message 2');
      const messageValue = message.value.toString();
      console.log(`Received message on topic ${topic}: ${messageValue}`);
      // sendMessageToClients(messageValue); // Send the Kafka message to WebSocket clients
      wss.broadcast(messageValue);
    },
  });
};

// Start the HTTPS server
const PORT = 30018; // Replace with your desired port number
server.listen(PORT, () => {
  console.log(`Server is listening on port ${PORT}`);
  runConsumer(); // Start the Kafka consumer when the server is ready
});
