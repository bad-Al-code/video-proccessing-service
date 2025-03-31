import {
  connectRabbitMQ,
  closeRabbitMQConnection,
} from './config/rabbitmq-client';
import {
  VIDEO_EVENTS_EXCHANGE,
  VIDEO_PROCESSING_QUEUE,
  VIDEO_UPLOAD_COMPLETED_ROUTING_KEY,
} from './config/constants';
import { VideoProcessingConsumer } from './consumers/VideoProcessingConsumer';

let consumer: VideoProcessingConsumer | null = null;

async function startService() {
  console.log('--- Video Processing Service Starting ---');
  try {
    await connectRabbitMQ();
    console.log('RabbitMQ connection ready.');

    consumer = new VideoProcessingConsumer(
      VIDEO_PROCESSING_QUEUE,
      VIDEO_EVENTS_EXCHANGE,
      VIDEO_UPLOAD_COMPLETED_ROUTING_KEY,
    );
    await consumer.start();

    console.log('--- Video Processing Service Started Successfully ---');
  } catch (error) {
    console.error('Failed to start Video Processing Service:', error);
    await shutdown(1);
  }
}

let isShuttingDown = false;
async function shutdown(exitCode = 0) {
  if (isShuttingDown) return;
  isShuttingDown = true;

  console.log('\n--- Video Processing Service Shutting Down ---');

  if (consumer) {
    consumer.stop();
  }

  await closeRabbitMQConnection();

  console.log('Shutdown complete.');
  process.exit(exitCode);
}

process.on('SIGINT', () => shutdown(0));
process.on('SIGTERM', () => shutdown(0));
process.on('uncaughtException', (error) => {
  console.error('Unhandled Exception:', error);
  shutdown(1);
});
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  shutdown(1);
});

startService();
