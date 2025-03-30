import { VIDEO_UPLOAD_COMPLETED_ROUTING_KEY } from './config/constants';
import {
  closeRabbitMQConnection,
  connectRabbitMQ,
} from './config/rabbitmq-client';
import { VideoProcessingConsumer } from './consumers/VideoProcessingConsumer';
import { handleVideoUploadEvent } from './handlers/videoProcessingHandler';

const QUEUE_NAME = 'video_processing_queue';
const BINDING_KEY = VIDEO_UPLOAD_COMPLETED_ROUTING_KEY;

async function main() {
  console.log('--- Video Processing Service Starting ---');
  let consumer: VideoProcessingConsumer | null = null;

  try {
    await connectRabbitMQ();
    consumer = new VideoProcessingConsumer(QUEUE_NAME, BINDING_KEY);
    await consumer.start(handleVideoUploadEvent);

    console.log('--- Video Processing Service Started Successfully ---');
  } catch (error) {
    console.error('Failed to start Video Processing Service:', error);

    await shutdown();
    process.exit(1);
  }
}

let isShuttingDown = false;
async function shutdown() {
  if (isShuttingDown) return;

  isShuttingDown = true;
  console.log('\n--- Video Processing Service Shutting Down ---');

  await closeRabbitMQConnection();
  console.log(`RabbitMQ connection closed`);
  console.log(`Shutdown complete.`);
}
process.on('SIGINT', async () => {
  await shutdown();
  process.exit(1);
});
process.on('SIGTERM', async () => {
  await shutdown();
  process.exit(1);
});

main();
