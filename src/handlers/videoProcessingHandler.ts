import { ConsumeMessage } from 'amqplib';
import { mkdir, rm } from 'node:fs/promises';
import { eq } from 'drizzle-orm';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

import { downloadFromS3, uploadToS3 } from '../utis/s3Utils';
import { ENV } from '../config/env';
import { generateThumbnail, transcodeToResolution } from '../utis/ffmpegUtils';
import { S3_PROCESSED_PREFIX, S3_THUMBNAIL_PREFIX } from '../config/constants';
import { VideoUploadPayload } from '../consumers/VideoProcessingConsumer';
import { db } from '../db';
import { videos, VideoStatus } from '../db/schema';

const TEMP_PROCESSING_DIR = join(tmpdir(), 'video-processing-temp');

export const handleVideoUploadEvent = async (
  payload: VideoUploadPayload,
  msg: ConsumeMessage,
): Promise<boolean> => {
  const { videoId, s3Key: originalS3Key, originalFilename } = payload;
  console.log(
    `[Handler: ${videoId}] Received event. Original S3 Key: ${originalS3Key}`,
  );

  const jobTempDir = join(TEMP_PROCESSING_DIR, videoId);
  const localOriginalPath = join(jobTempDir, originalFilename || videoId);

  let dbStatus: VideoStatus = 'ERROR';
  let processedKeys: { [resolution: string]: string } = {};
  let thumbnailKey: string | null = null;

  try {
    await mkdir(jobTempDir, { recursive: true });
    console.log(`[Handler: ${videoId}] Created temp directory: ${jobTempDir}`);

    console.log(
      `[Handler: ${videoId}] Attempting to set status to PROCESSING...`,
    );
    await db
      .update(videos)
      .set({ status: 'PROCESSING' })
      .where(eq(videos.id, videoId));
    console.log(`[Handler: ${videoId}] Status successfully set to PROCESSING.`);

    dbStatus = 'PROCESSING';

    await downloadFromS3(
      ENV.AWS_S3_BUCKET_NAME,
      originalS3Key,
      localOriginalPath,
    );
    console.log(`[Handler: ${videoId}] Starting parallel processing tasks...`);

    const tasks: Promise<any>[] = [];

    const targetResolutions: ('720p' | '480p')[] = ['480p', '720p'];

    for (const res of targetResolutions) {
      tasks.push(
        transcodeToResolution(localOriginalPath, jobTempDir, res, videoId).then(
          (result) => {
            const outputS3Key = `${S3_PROCESSED_PREFIX}${videoId}/${videoId}_${result.resolution}.mp4`;
            return uploadToS3(
              ENV.AWS_S3_BUCKET_NAME,
              outputS3Key,
              result.outputPath,
              'video/mp4',
            );
          },
        ),
      );
    }

    tasks.push(
      generateThumbnail(localOriginalPath, jobTempDir, videoId).then(
        (thumbnailPath) => {
          thumbnailKey = `${S3_THUMBNAIL_PREFIX}${videoId}/${videoId}_thumbnail.jpg`;
          return uploadToS3(
            ENV.AWS_S3_BUCKET_NAME,
            thumbnailKey,
            thumbnailPath,
            'image/jpeg',
          );
        },
      ),
    );

    await Promise.all(tasks);
    console.log(
      `[Handler: ${videoId}] All processing and S3 uploads complete.`,
    );

    dbStatus = 'READY';
    console.log(
      `[Handler: ${videoId}] Attempting final DB update (Status: READY)...`,
    );

    await db
      .update(videos)
      .set({ status: 'READY' })
      .where(eq(videos.id, videoId));

    console.log(`[Handler: ${videoId}] Final DB update successful.`);

    return true;
  } catch (error: any) {
    console.error(
      `[Handler: ${videoId}] ERROR during processing:`,
      error.message || error,
    );

    dbStatus = 'ERROR';

    return false;
  } finally {
    console.log(
      `[Handler: ${videoId}] Cleaning up temporary directory: ${jobTempDir}`,
    );
    await rm(jobTempDir, { recursive: true, force: true }).catch((err) => {
      console.error(
        `[Handler: ${videoId}] Error cleaning up temp directory ${jobTempDir}:`,
        err,
      );
    });

    if (dbStatus === 'ERROR') {
      try {
        console.warn(
          `[Handler: ${videoId}] Attempting to set status to ERROR in DB...`,
        );
        await db
          .update(videos)
          .set({ status: 'ERROR' })
          .where(eq(videos.id, videoId));
        console.warn(`[Handler: ${videoId}] Status set to ERROR in DB.`);
      } catch (dbError: any) {
        console.error(
          `[Handler: ${videoId}] FAILED to update status to ERROR in DB:`,
          dbError.message || dbError,
        );
      }
    }
    console.log(`[Handler: ${videoId}] Finished processing.`);
  }
};
