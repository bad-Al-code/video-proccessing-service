import { Buffer } from 'node:buffer';

import { getRabbitMQChannel } from '../config/rabbitmq-client';
import { VIDEO_EVENTS_EXCHANGE } from '../config/constants';
import { logger } from '../config/logger';

export class VideoEventProducer {
  constructor() {
    logger.info('[VideoEventProducer] Producer initialized.');
  }

  /**
   * Publishes an event to the video events topic exchange.
   * @param routingKey The routing key for the event (e.g., 'video.processing.completed').
   * @param eventPayload The data payload for the event.
   * @returns Promise resolving to true if publish was buffered, false otherwise (e.g., channel closed/full).
   */
  async publishVideoEvent(
    routingKey: string,
    eventPayload: any,
  ): Promise<boolean> {
    const channel = getRabbitMQChannel();
    const logCtx = { routingKey, videoId: eventPayload?.videoId };

    if (!channel) {
      logger.error(
        logCtx,
        '[VideoEventProducer] Cannot publish event, channel is not available.',
      );
      return false;
    }

    try {
      const messageString = JSON.stringify(eventPayload);
      const messageBuffer = Buffer.from(messageString);

      logger.debug(
        { ...logCtx, payloadSize: messageBuffer.length },
        `[VideoEventProducer] Publishing event payload`,
      );

      const sent = channel.publish(
        VIDEO_EVENTS_EXCHANGE,
        routingKey,
        messageBuffer,
        { persistent: true, contentType: 'application/json' },
      );

      if (sent) {
        logger.info(
          logCtx,
          `[VideoEventProducer] Published event successfully to exchange '${VIDEO_EVENTS_EXCHANGE}'`,
        );
      } else {
        logger.warn(
          logCtx,
          `[VideoEventProducer] Failed to publish event (channel buffer full or closing?). Event lost.`,
        );
      }
      return sent;
    } catch (error: any) {
      logger.error(
        { ...logCtx, err: error.message, stack: error.stack },
        `[VideoEventProducer] Error publishing event`,
      );
      return false;
    }
  }
}

export const videoEventProducer = new VideoEventProducer();
