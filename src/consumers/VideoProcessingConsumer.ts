import { Channel, ConsumeMessage } from 'amqplib';
import { connectRabbitMQ, getRabbitMQChannel } from '../config/rabbitmq-client';
import { VIDEO_EVENTS_EXCHANGE } from '../config/constants';

export interface VideoUploadPayload {
  videoId: string;
  s3Key: string;
  originalFilename: string;
  mimeType: string;
}

type VideoMessageHandler = (
  payload: VideoUploadPayload,
  msg: ConsumeMessage,
) => Promise<boolean>;

export class VideoProcessingConsumer {
  private channel: Channel | null = null;

  constructor(
    private queueName: string,
    private bindingKey: string,
  ) {
    console.log(
      `[Consumer] Initialized for queue '${queueName}' with binding key '${bindingKey}'.`,
    );
  }

  async start(onMessage: VideoMessageHandler): Promise<void> {
    try {
      this.channel = getRabbitMQChannel();

      if (!this.channel) {
        console.warn('[Consumer] Channel not found, attempting connection...');
        const { channel } = await connectRabbitMQ();
        this.channel = channel;
      }

      console.log(
        `[Consumer] Asserting exchange '${VIDEO_EVENTS_EXCHANGE}' and queue '${this.queueName}'...`,
      );
      await this.channel.assertExchange(VIDEO_EVENTS_EXCHANGE, 'topic', {
        durable: true,
      });

      await this.channel.assertQueue(this.queueName, { durable: true });

      await this.channel.bindQueue(
        this.queueName,
        VIDEO_EVENTS_EXCHANGE,
        this.bindingKey,
      );
      console.log(
        `[Consumer] Queue '${this.queueName}' bound to exchange '${VIDEO_EVENTS_EXCHANGE}' with key '${this.bindingKey}'.`,
      );

      await this.channel.prefetch(1);
      console.log(`[Consumer] QoS Prefetch set to 1.`);

      console.log(
        `[Consumer] Waiting for messages matching '${this.bindingKey}'. To exit press CTRL+C`,
      );

      this.channel.consume(
        this.queueName,
        async (msg: ConsumeMessage | null) => {
          if (msg !== null && this.channel) {
            let payload: VideoUploadPayload | null = null;
            try {
              console.log(
                `[Consumer] Received message with routingKey: '${msg.fields.routingKey}'`,
              );
              const messageString = msg.content.toString();
              payload = JSON.parse(messageString) as VideoUploadPayload;

              if (
                !payload ||
                typeof payload.videoId !== 'string' ||
                typeof payload.s3Key !== 'string'
              ) {
                throw new Error('Invalid message payload structure.');
              }

              console.log(`[Consumer] Processing videoId: ${payload.videoId}`);
              const success = await onMessage(payload, msg);

              if (success) {
                this.channel.ack(msg);
                console.log(
                  `[Consumer] Acknowledged message for videoId: ${payload.videoId}`,
                );
              } else {
                console.warn(
                  `[Consumer] Handler failed for videoId: ${payload.videoId}. Rejecting (nack) without requeue.`,
                );
                this.channel.nack(msg, false, false);
              }
            } catch (error: any) {
              const videoId = payload?.videoId || 'unknown';
              console.error(
                `[Consumer] Error processing message for videoId ${videoId}:`,
                error.message || error,
              );
              console.error(
                '[Consumer] Original Message Content:',
                msg.content.toString(),
              );
              this.channel.nack(msg, false, false);
            }
          } else if (msg === null) {
            console.warn(
              '[Consumer] Consume callback received null message. Channel or connection likely closed.',
            );
          }
        },
        { noAck: false },
      );
    } catch (error: any) {
      console.error(
        `[Consumer] Error starting consumer:`,
        error.message || error,
      );
      throw error;
    }
  }
}
