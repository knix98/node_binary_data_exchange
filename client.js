const net = require('net');
const fs = require('fs');
const { Buffer } = require('buffer');

class StockTickerClient {
    constructor(serverHost, serverPort) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.packets = [];
        this.missingSequences = new Set();
        this.currentMissingSequence = null;
        this.PACKET_SIZE = 17;
    }

    run() {
        this.createClient(this.streamAllPackets.bind(this), this.handleInitialData.bind(this), this.handleInitialClose.bind(this));
    }
    
    createClient(onConnect, onData, onClose) {
        const client = new net.Socket();
        client.connect(this.serverPort, this.serverHost, () => onConnect(client));
        client.on('data', onData);
        client.on('close', onClose);
        client.on('error', (err) => {
            this.handleError(err, client);
        });
        return client;
    }

    streamAllPackets(client) {
        console.log('Connected to the server and requesting all packets');
        const buffer = Buffer.alloc(2);
        buffer.writeUInt8(1, 0); // callType = 1
        buffer.writeUInt8(0, 1); // resendSeq (not applicable here for all packets)
        client.write(buffer);
    }

    parsePackets(data) {
        let offset = 0;
        while (offset < data.length) {
          if (data.length - offset < this.PACKET_SIZE) {
            console.log('Incomplete packet received, discarding.');
            break;
          }
    
          const symbol = data.toString('ascii', offset, offset + 4);
          const buySell = data.toString('ascii', offset + 4, offset + 5);
          const quantity = data.readUInt32BE(offset + 5);
          const price = data.readUInt32BE(offset + 9);
          const sequence = data.readUInt32BE(offset + 13);
    
          if (!/^[A-Z]{4}$/.test(symbol)) {
            console.log(`Invalid symbol: ${symbol}, discarding packet.`);
            offset += this.PACKET_SIZE;
            continue;
          }
          if (!/^[BS]$/.test(buySell)) {
            console.log(`Invalid buy/sell indicator: ${buySell}, discarding packet.`);
            offset += this.PACKET_SIZE;
            continue;
          }
          if (!Number.isInteger(quantity) || quantity <= 0) {
            console.log(`Invalid quantity: ${quantity}, discarding packet.`);
            offset += this.PACKET_SIZE;
            continue;
          }
          if (!Number.isInteger(price) || price <= 0) {
            console.log(`Invalid price: ${price}, discarding packet.`);
            offset += this.PACKET_SIZE;
            continue;
          }
          if (!Number.isInteger(sequence) || sequence <= 0) {
            console.log(`Invalid sequence: ${sequence}, discarding packet.`);
            offset += this.PACKET_SIZE;
            continue;
          }
    
          const packet = { symbol, buySell, quantity, price, sequence };
          this.packets.push(packet);
          offset += this.PACKET_SIZE;
        }
    }

    identifyMissingSequences() {
        this.packets.sort((a, b) => a.sequence - b.sequence);
        let expectedSequence = 1;
        for (const packet of this.packets) {
          while (packet.sequence > expectedSequence) {
            this.missingSequences.add(expectedSequence);
            expectedSequence++;
          }
          expectedSequence++;
        }
    }
    
    requestNextMissingPacket(client) {
        if (this.missingSequences.size === 0) {
            client.end();
            return;
        }

        this.currentMissingSequence = [...this.missingSequences][0];
        const buffer = Buffer.alloc(2);
        buffer.writeUInt8(2, 0); // callType = 2
        buffer.writeUInt8(this.currentMissingSequence, 1); // resendSeq
        client.write(buffer);
    }
    
    handleMissingPacket(data) {
        this.parsePackets(data);
        this.missingSequences.delete(this.currentMissingSequence);
        this.currentMissingSequence = null;
    }

    generateJSON() {
        const sortedPackets = this.packets.sort((a, b) => a.sequence - b.sequence);
        const jsonContent = JSON.stringify(sortedPackets, null, 2);
        fs.writeFile('output.json', jsonContent, 'utf8', (err) => {
            if (err) {
                console.log('An error occurred while writing JSON Object to File.');
                console.log(err);
            } else {
                console.log('JSON file has been saved.');
            }
        });
    }
    
    handleInitialData(data) {
        this.parsePackets(data);
    }

    handleInitialClose() {
        console.log('Connection closed by the server');
        this.identifyMissingSequences();

        // Reconnect to request missing packets
        if (this.missingSequences.size > 0) {
            const missingClient = this.createClient((client) => {
                console.log('Reconnected to request missing packets');
                this.requestNextMissingPacket(client);
            }, (data) => {
                this.handleMissingPacket(data);
                this.requestNextMissingPacket(missingClient);
            }, () => {
                console.log('Finished requesting missing packets');
                this.generateJSON();
            });
        } else {
            this.generateJSON();
        }
    }

    handleError(err, client) {
        console.log('Client error:', err);
        client.end();
    }
}

const client = new StockTickerClient('localhost', 3000);
client.run();