import { createClient } from 'redis';

interface TaskData {
    taskData: string;
    userChannel: string;
}

const Redis = createClient({
    password: 'L07dyz33z8RUKuUBLGcgIxqfY46IAxZs',
    socket: {
        host: 'redis-15911.c305.ap-south-1-1.ec2.redns.redis-cloud.com',
        port: 15911,
    },
});

async function startWorker(): Promise<void> {
    try {
        await Redis.connect();
        console.log('Worker connected to Redis');

        while (true) {
            const requestData = await Redis.rPop('requests');
            if (requestData) {
                const { taskData, userChannel }: TaskData = JSON.parse(requestData);
                console.log('Processing request:', { taskData });

                try {
                    const response = await callExternalApi({ taskData });
                    const cacheKey = `${taskData}`;
                    await Redis.set(cacheKey, JSON.stringify(response), { EX: 3600 });
                    console.log('Request processed and response cached:', response);
                    await Redis.publish(userChannel, JSON.stringify({ cacheKey, response }));

                } catch (error) {
                    console.error('API call failed:', error);
                }
            } else {
                console.log('No requests in the queue, worker is waiting...');
                await sleep(1000);
            }
        }
    } catch (e) {
        console.log('Error occurred in worker:', e);
    }
}

async function callExternalApi({ taskData }: { taskData: string }) {
    console.log('Calling external API...');
    return { success: true, data: `Response for taskData: ${taskData}` };
}

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

startWorker().catch(console.error);
