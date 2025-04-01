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
import { closeDbConnection } from './db';
import { logger } from './config/logger';

let consumer: VideoProcessingConsumer | null = null;

async function startService() {
  logger.info('--- Video Processing Service Starting ---');
  try {
    await connectRabbitMQ();
    logger.info('RabbitMQ connection ready.');

    consumer = new VideoProcessingConsumer(
      VIDEO_PROCESSING_QUEUE,
      VIDEO_EVENTS_EXCHANGE,
      VIDEO_UPLOAD_COMPLETED_ROUTING_KEY,
    );
    await consumer.start();

    logger.info('--- Video Processing Service Started Successfully ---');
  } catch (error) {
    logger.error('Failed to start Video Processing Service:', error);
    await shutdown(1);
  }
}

let isShuttingDown = false;
async function shutdown(exitCode = 0) {
  if (isShuttingDown) return;
  isShuttingDown = true;

  logger.info('\n--- Video Processing Service Shutting Down ---');

  if (consumer) {
    consumer.stop();
  }

  await closeRabbitMQConnection();

  await closeDbConnection();

  logger.info('Shutdown complete.');
  process.exit(exitCode);
}

process.on('SIGINT', () => shutdown(0));
process.on('SIGTERM', () => shutdown(0));
process.on('uncaughtException', (error) => {
  logger.error('Unhandled Exception:', error);
  shutdown(1);
});
process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
  shutdown(1);
});

startService();
