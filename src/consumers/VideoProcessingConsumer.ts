import { Channel, ConsumeMessage } from 'amqplib';

import { logger } from '../config/logger';
import { getRabbitMQChannel } from '../config/rabbitmq-client';
import { handleVideoUploadEvent } from '../handlers/videoProcessingHandler';
import {
  VIDEO_PROCESSING_DLQ,
  VIDEO_PROCESSING_DLX,
} from '../config/constants';

export interface VideoUploadPayload {
  videoId: string;
  s3Key: string;
  originalFilename: string;
  mimeType: string;
}

export class VideoProcessingConsumer {
  private channel: Channel | null = null;
  private isRunning = false;

  constructor(
    private queueName: string,
    private exchangeName: string,
    private bindingKey: string,
  ) {}

  async start(): Promise<void> {
    if (this.isRunning) {
      logger.warn('[Consumer] Already running.');
      return;
    }

    try {
      logger.info('[Consumer] Starting...');
      this.channel = getRabbitMQChannel();
      if (!this.channel) {
        throw new Error('[Consumer] Cannot start without a RabbitMQ channel.');
      }

      logger.info(
        `[Consumer] Asserting Dead Letter Exchange '${VIDEO_PROCESSING_DLX}' (direct, durable)`,
      );
      await this.channel.assertExchange(VIDEO_PROCESSING_DLX, 'direct', {
        durable: true,
      });

      logger.info(
        `[Consumer] Asserting Dead Letter Queue '${VIDEO_PROCESSING_DLQ}' (durable)`,
      );
      const dlq = await this.channel.assertQueue(VIDEO_PROCESSING_DLQ, {
        durable: true,
      });

      logger.info(
        `[Consumer] Binding DLQ '${dlq.queue}' to DLX '${VIDEO_PROCESSING_DLX}' with binding key '${this.bindingKey}'`,
      );
      await this.channel.bindQueue(
        dlq.queue,
        VIDEO_PROCESSING_DLX,
        this.bindingKey,
      );

      logger.info(
        `[Consumer] Asserting MAIN queue '${this.queueName}' (durable) with DLX configured`,
      );
      const q = await this.channel.assertQueue(this.queueName, {
        durable: true,
        arguments: {
          'x-dead-letter-exchange': VIDEO_PROCESSING_DLX,
          'x-dead-letter-routing-key': this.bindingKey,
        },
      });

      logger.info(
        `[Consumer] Asserting MAIN exchange '${this.exchangeName}' (topic, durable)`,
      );
      await this.channel.assertExchange(this.exchangeName, 'topic', {
        durable: true,
      });

      logger.info(
        `[Consumer] Binding MAIN queue '${q.queue}' to exchange '${this.exchangeName}' with key '${this.bindingKey}'`,
      );
      await this.channel.bindQueue(q.queue, this.exchangeName, this.bindingKey);

      await this.channel.prefetch(1);
      logger.info('[Consumer] QoS prefetch set to 1.');

      logger.info(
        `[Consumer] Waiting for messages on queue '${q.queue}'. To exit press CTRL+C`,
      );
      this.isRunning = true;

      this.channel.consume(q.queue, this.processMessage.bind(this), {
        noAck: false,
      });
    } catch (error: any) {
      logger.error('[Consumer] Error starting consumer:', error.message);
      this.isRunning = false;
      throw error;
    }
  }

  private async processMessage(msg: ConsumeMessage | null): Promise<void> {
    if (msg === null) {
      logger.warn(
        '[Consumer] Received null message, queue might have been deleted or channel closed.',
      );
      return;
    }

    if (!this.channel) {
      logger.error(
        '[Consumer] Channel is null, cannot process message or nack.',
      );

      return;
    }

    let payload: VideoUploadPayload | null = null;
    const contentString = msg.content.toString();

    try {
      logger.info(
        `[Consumer] Received message [${msg.fields.deliveryTag}] RoutingKey: ${msg.fields.routingKey}`,
      );
      payload = JSON.parse(contentString) as VideoUploadPayload;

      if (
        !payload ||
        typeof payload.videoId !== 'string' ||
        typeof payload.s3Key !== 'string'
      ) {
        throw new Error('Invalid message payload structure.');
      }

      logger.info(`[Consumer] Processing videoId: ${payload.videoId}`);
      const success = await handleVideoUploadEvent(payload, msg);

      if (!this.channel) {
        logger.error(
          '[Consumer] Channel became null after processing, cannot ack/nack.',
        );
        return;
      }

      if (success) {
        logger.info(
          `[Consumer] Acknowledged message [${msg.fields.deliveryTag}] for videoId: ${payload.videoId}`,
        );
        this.channel.ack(msg);
      } else {
        logger.warn(
          `[Consumer] Handler failed for videoId: ${payload.videoId}. Rejecting [${msg.fields.deliveryTag}] (nack) without requeue.`,
        );
        this.channel.nack(msg, false, false);
      }
    } catch (error: any) {
      const videoId = payload?.videoId || 'unknown';
      logger.error(
        `[Consumer] Error processing message for videoId ${videoId} [${msg.fields.deliveryTag}]:`,
        error.message || error,
      );
      logger.error('[Consumer] Message Content:', contentString);

      if (this.channel) {
        this.channel.nack(msg, false, false);
      } else {
        logger.error(
          `[Consumer] Channel is null, cannot nack message for videoId ${videoId} after error.`,
        );
      }
    }
  }

  stop() {
    this.isRunning = false;
    logger.info('[Consumer] Stopping...');
  }
}
