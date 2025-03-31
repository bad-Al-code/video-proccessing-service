import { Channel, ConsumeMessage } from 'amqplib';
import { getRabbitMQChannel } from '../config/rabbitmq-client';
import { handleVideoUploadEvent } from '../handlers/VideoProcessingHandler';

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
      console.warn('[Consumer] Already running.');
      return;
    }

    try {
      console.log('[Consumer] Starting...');
      this.channel = getRabbitMQChannel();
      if (!this.channel) {
        throw new Error('[Consumer] Cannot start without a RabbitMQ channel.');
      }

      console.log(
        `[Consumer] Asserting exchange '${this.exchangeName}' (topic, durable)`,
      );
      await this.channel.assertExchange(this.exchangeName, 'topic', {
        durable: true,
      });

      console.log(`[Consumer] Asserting queue '${this.queueName}' (durable)`);
      const q = await this.channel.assertQueue(this.queueName, {
        durable: true,
      });

      console.log(
        `[Consumer] Binding queue '${q.queue}' to exchange '${this.exchangeName}' with key '${this.bindingKey}'`,
      );
      await this.channel.bindQueue(q.queue, this.exchangeName, this.bindingKey);

      await this.channel.prefetch(1);
      console.log('[Consumer] QoS prefetch set to 1.');

      console.log(
        `[Consumer] Waiting for messages on queue '${q.queue}'. To exit press CTRL+C`,
      );
      this.isRunning = true;

      this.channel.consume(q.queue, this.processMessage.bind(this), {
        noAck: false,
      });
    } catch (error: any) {
      console.error('[Consumer] Error starting consumer:', error.message);
      this.isRunning = false;
      throw error;
    }
  }

  private async processMessage(msg: ConsumeMessage | null): Promise<void> {
    if (msg === null) {
      console.warn(
        '[Consumer] Received null message, queue might have been deleted or channel closed.',
      );
      return;
    }

    if (!this.channel) {
      console.error(
        '[Consumer] Channel is null, cannot process message or nack.',
      );

      return;
    }

    let payload: VideoUploadPayload | null = null;
    const contentString = msg.content.toString();

    try {
      console.log(
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

      console.log(`[Consumer] Processing videoId: ${payload.videoId}`);
      const success = await handleVideoUploadEvent(payload, msg);

      if (!this.channel) {
        console.error(
          '[Consumer] Channel became null after processing, cannot ack/nack.',
        );
        return;
      }

      if (success) {
        console.log(
          `[Consumer] Acknowledged message [${msg.fields.deliveryTag}] for videoId: ${payload.videoId}`,
        );
        this.channel.ack(msg);
      } else {
        console.warn(
          `[Consumer] Handler failed for videoId: ${payload.videoId}. Rejecting [${msg.fields.deliveryTag}] (nack) without requeue.`,
        );
        this.channel.nack(msg, false, false);
      }
    } catch (error: any) {
      const videoId = payload?.videoId || 'unknown';
      console.error(
        `[Consumer] Error processing message for videoId ${videoId} [${msg.fields.deliveryTag}]:`,
        error.message || error,
      );
      console.error('[Consumer] Message Content:', contentString);

      if (this.channel) {
        this.channel.nack(msg, false, false);
      } else {
        console.error(
          `[Consumer] Channel is null, cannot nack message for videoId ${videoId} after error.`,
        );
      }
    }
  }

  stop() {
    this.isRunning = false;
    console.log('[Consumer] Stopping...');
  }
}
