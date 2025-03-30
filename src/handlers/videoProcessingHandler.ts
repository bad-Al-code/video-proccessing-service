import { ConsumeMessage } from 'amqplib';
import { VideoUploadPayload } from '../consumers/VideoProcessingConsumer';
import { db } from '../db';
import { videos } from '../db/schema';
import { eq } from 'drizzle-orm';

export const handleVideoUploadEvent = async (
  payload: VideoUploadPayload,
  msg: ConsumeMessage,
): Promise<boolean> => {
  const { videoId, s3Key } = payload;

  try {
    await db
      .update(videos)
      .set({ status: 'PROCESSING' })
      .where(eq(videos.id, videoId));

    await new Promise((resolve) => setTimeout(resolve, 3000));

    console.log(
      `[Handler] Simulated processing complete for videoId: ${videoId}`,
    );

    console.log(`[Handler] Updating status to READY for videoId: ${videoId}`);

    await db
      .update(videos)
      .set({ status: 'READY' })
      .where(eq(videos.id, videoId));

    return true;
  } catch (error: any) {
    console.error(
      `[Handler] Error processing videoId ${videoId}:`,
      error.message || error,
    );
    try {
      console.error(
        `[Handler] Attempting to set status to ERROR for videoId: ${videoId}`,
      );
      await db
        .update(videos)
        .set({ status: 'ERROR' })
        .where(eq(videos.id, videoId));
      console.warn(`[Handler] Status updated to ERROR for videoId: ${videoId}`);
    } catch (dbError: any) {
      console.error(
        `[Handler] Failed to update status to ERROR for videoId ${videoId}:`,
        dbError.message || dbError,
      );
    }

    return false;
  }
};
