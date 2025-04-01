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

    if (!channel) {
      logger.error(
        `[VideoEventProducer] Cannot publish event, channel is not available. RoutingKey: ${routingKey}`,
      );
      return false;
    }

    try {
      await channel.assertExchange(VIDEO_EVENTS_EXCHANGE, 'topic', {
        durable: true,
      });

      const messageBuffer = Buffer.from(JSON.stringify(eventPayload));

      const sent = channel.publish(
        VIDEO_EVENTS_EXCHANGE,
        routingKey,
        messageBuffer,
        { persistent: true },
      );

      if (sent) {
        logger.info(
          `[VideoEventProducer] Published event to exchange '${VIDEO_EVENTS_EXCHANGE}' [${routingKey}]:`,
          JSON.stringify(eventPayload).substring(0, 200) +
            (JSON.stringify(eventPayload).length > 200 ? '...' : ''),
        );
      } else {
        logger.warn(
          `[VideoEventProducer] Failed to publish event [${routingKey}] (channel buffer full or closing?). Payload:`,
          eventPayload,
        );
      }
      return sent;
    } catch (error: any) {
      logger.error(
        `[VideoEventProducer] Error publishing event [${routingKey}]:`,
        error.message || error,
      );
      logger.error(`[VideoEventProducer] Payload:`, eventPayload);
      return false;
    }
  }
}

export const videoEventProducer = new VideoEventProducer();
