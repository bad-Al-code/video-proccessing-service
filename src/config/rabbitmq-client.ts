import amqp, {
  Connection as AmqpConnection,
  Channel,
  ChannelModel,
} from 'amqplib';
import { ENV } from './env';
import { logger } from './logger';

const RMQ_USER = ENV.RABBITMQ_USER;
const RMQ_PASS = ENV.RABBITMQ_PASSWORD;
const RMQ_HOST = ENV.RABBITMQ_HOST;
const RMQ_PORT = ENV.RABBITMQ_NODE_PORT;
const RMQ_VHOST = ENV.RABBITMQ_VHOST;

const connectionUrl = `amqp://${RMQ_USER}:${RMQ_PASS}@${RMQ_HOST}:${RMQ_PORT}${RMQ_VHOST}`;

interface RabbitMQHandles {
  model: ChannelModel;
  channel: Channel;
}
let connectionModelInstance: ChannelModel | null = null;
let channelInstance: Channel | null = null;

export async function connectRabbitMQ(): Promise<RabbitMQHandles> {
  if (connectionModelInstance && channelInstance) {
    logger.info(`Reusing existing RabbitMQ model and channel.`);
    return { model: connectionModelInstance, channel: channelInstance };
  }

  try {
    const model: ChannelModel = await amqp.connect(connectionUrl);
    connectionModelInstance = model;

    model.on('error', (err: Error) => {
      logger.error('RabbitMQ connection model error:', err.message);
      connectionModelInstance = null;
      channelInstance = null;
    });

    model.on('close', () => {
      logger.warn('RabbitMQ connection model closed.');
      connectionModelInstance = null;
      channelInstance = null;
    });

    logger.info('RabbitMQ connection model created!');

    const channel: Channel = await model.createChannel();
    channelInstance = channel;
    logger.info(`RabbitMQ channel created`);

    channel.on('error', (err: Error) => {
      logger.error('RabbitMQ channel model error:', err.message);
      connectionModelInstance = null;
      channelInstance = null;
    });

    channel.on('close', () => {
      logger.warn('RabbitMQ channel model closed.');
      connectionModelInstance = null;
      channelInstance = null;
    });

    return { model, channel };
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error('Failed to connect to RabbitMQ: ', errorMessage);

    connectionModelInstance = null;
    channelInstance = null;

    throw new Error(`RabbitMQ connection failed: ${errorMessage}`);
  }
}

export async function closeRabbitMQConnection(): Promise<void> {
  try {
    if (channelInstance) {
      await channelInstance.close();
      channelInstance = null;
      logger.info('RabbitMQ channel closed.');
    }

    if (connectionModelInstance) {
      await connectionModelInstance.close();
      connectionModelInstance = null;
      logger.info('RabbitMQ connection model closed.');
    }
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error('Error closing RabbitMQ resources:', errorMessage);
    channelInstance = null;
    connectionModelInstance = null;
  } finally {
    channelInstance = null;
    connectionModelInstance = null;
  }
}

export function getRabbitMQChannel(): Channel | null {
  return channelInstance;
}

export function getRabbitMQConnection(): ChannelModel | null {
  return connectionModelInstance;
}
