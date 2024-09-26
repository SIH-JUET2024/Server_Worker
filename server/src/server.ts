import express from 'express';
import { WebSocketServer } from 'ws';
import { createClient } from 'redis';
import { v4 as uuidv4 } from 'uuid';

const Redis = createClient({
    password: 'L07dyz33z8RUKuUBLGcgIxqfY46IAxZs',
    socket: {
        host: 'redis-15911.c305.ap-south-1-1.ec2.redns.redis-cloud.com',
        port: 15911,
    },
});

startServer();
Redis.on('error', err => console.log('Redis Client Error', err));

const app = express();
const httpServer = app.listen(4000);

const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', function connection(ws) {
    ws.on('error', console.error);

    ws.on('message', async function message(data) {
        const { taskData, userId } = JSON.parse(data.toString());
        if (taskData && userId) {
            const cacheKey = `${taskData}`;
            const cachedResponse = await Redis.get(cacheKey);
            if (cachedResponse) {
                console.log('Returning cached response');
                ws.send(cachedResponse);
            } else {
                const userChannel = `${userId}-${uuidv4()}`;
                const requestData = JSON.stringify({ taskData, userChannel });
                await Redis.lPush('requests', requestData);
                console.log('Request queued:', taskData);
                ws.send('Request queued. Awaiting processing...');

                const subscriber = Redis.duplicate();
                await subscriber.connect();
                await subscriber.subscribe(userChannel, (message) => {
                    const { cacheKey: responseCacheKey, response } = JSON.parse(message);

                    if (responseCacheKey === cacheKey) {
                        ws.send(JSON.stringify(response));
                        console.log('Response sent to client:', response);

                        subscriber.unsubscribe(userChannel);
                        subscriber.quit();
                    }
                });
            }
        } else {
            console.log('Invalid request data');
        }
    });

    ws.send('Connection Established');
});

async function startServer() {
    try {
        await Redis.connect();
        console.log('Connected to Redis');
    } catch (e) {
        console.log('Error occurred:', e);
    }
}
