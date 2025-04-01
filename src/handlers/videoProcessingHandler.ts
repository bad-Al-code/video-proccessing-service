import { ConsumeMessage } from 'amqplib';
import { eq } from 'drizzle-orm';
import { mkdir, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

import { VideoUploadPayload } from '../consumers/VideoProcessingConsumer';
import { db } from '../db';
import { videos, VideoStatus } from '../db/schema';
import { ENV } from '../config/env';
import {
  S3_PROCESSED_PREFIX,
  S3_THUMBNAIL_PREFIX,
  VIDEO_PROCESSING_COMPLETED_ROUTING_KEY,
  VIDEO_PROCESSING_FAILED_ROUTING_KEY,
} from '../config/constants';
import { downloadFromS3, uploadToS3 } from '../utils/s3.util';
import {
  generateThumbnail,
  transcodeToResolution,
  TranscodeResult,
  ThumbnailResult,
} from '../utils/ffmpeg.util';
import { videoEventProducer } from '../producers/videoEvent.producer';

const TEMP_BASE_DIR = join(tmpdir(), 'video-processing-service');

export const handleVideoUploadEvent = async (
  payload: VideoUploadPayload,
  msg: ConsumeMessage,
): Promise<boolean> => {
  const { videoId, s3Key: originalS3Key, originalFilename } = payload;
  const safeOriginalFilename = (originalFilename || videoId).replace(
    /[^a-zA-Z0-9._-]/g,
    '_',
  );
  const jobTempDir = join(TEMP_BASE_DIR, videoId);
  const localOriginalPath = join(
    jobTempDir,
    `original_${safeOriginalFilename}`,
  );

  try {
    const existingVideo = await db.query.videos.findFirst({
      columns: { status: true },
      where: eq(videos, videoId),
    });

    if (
      existingVideo?.status &&
      ['PROCESSING', 'READY', 'ERROR'].includes(existingVideo.status)
    ) {
      console.warn(
        `[Handler:${videoId}] Video already processed or is processing (status: ${existingVideo.status}). Skipping duplicate message.`,
      );

      return true;
    }

    if (!existingVideo) {
      console.error(
        `[Handler:${videoId}] Video record not found in DB. Cannot process. NACKing message.`,
      );

      await videoEventProducer.publishVideoEvent(
        VIDEO_PROCESSING_FAILED_ROUTING_KEY,
        {
          videoId: videoId,
          status: 'ERROR',
          error: 'Video record not found in database',
          originalS3Key,
        },
      );

      return false;
    }
  } catch (dbError: any) {
    console.error(
      `[Handler:${videoId}] DB error during idempotency check:`,
      dbError.message,
    );

    return false;
  }

  console.log(`[Handler:${videoId}] Processing job. Temp dir: ${jobTempDir}`);

  let dbStatusUpdate: {
    status: VideoStatus;
    processedAt?: Date;
    s3Key720p?: string | null;
    s3Key480p?: string | null;
    s3KeyThumbnail?: string | null;
  } | null = null;
  let processingError: Error | null = null;
  let finalS3Keys = {
    s3Key720p: `${S3_PROCESSED_PREFIX}${videoId}/${videoId}_720p.mp4`,
    s3Key480p: `${S3_PROCESSED_PREFIX}${videoId}/${videoId}_480p.mp4`,
    s3KeyThumbnail: `${S3_THUMBNAIL_PREFIX}${videoId}/${videoId}_thumbnail.jpg`,
  };

  try {
    await mkdir(TEMP_BASE_DIR, { recursive: true });
    await mkdir(jobTempDir, { recursive: true });
    console.log(`[Handler:${videoId}] Created temp directory.`);

    console.log(`[Handler:${videoId}] Setting status to PROCESSING...`);
    await db
      .update(videos)
      .set({ status: 'PROCESSING' })
      .where(eq(videos.id, videoId));
    console.log(`[Handler:${videoId}] Status set to PROCESSING.`);

    await downloadFromS3(
      ENV.AWS_S3_BUCKET_NAME,
      originalS3Key,
      localOriginalPath,
    );
    console.log(`[Handler:${videoId}] Original video downloaded.`);

    console.log(`[Handler:${videoId}] Starting parallel processing tasks...`);
    const processingTasks: [
      Promise<TranscodeResult>,
      Promise<TranscodeResult>,
      Promise<ThumbnailResult>,
    ] = [
      transcodeToResolution(
        localOriginalPath,
        jobTempDir,
        '720p',
        videoId,
        finalS3Keys.s3Key720p,
      ),
      transcodeToResolution(
        localOriginalPath,
        jobTempDir,
        '480p',
        videoId,
        finalS3Keys.s3Key480p,
      ),
      generateThumbnail(
        localOriginalPath,
        jobTempDir,
        videoId,
        finalS3Keys.s3KeyThumbnail,
      ),
    ];

    const [result720p, result480p, resultThumbnail] =
      await Promise.all(processingTasks);
    console.log(`[Handler:${videoId}] FFmpeg processing complete.`);

    console.log(
      `[Handler:${videoId}] Starting parallel S3 uploads for processed files...`,
    );
    const uploadTasks = [
      uploadToS3(
        ENV.AWS_S3_BUCKET_NAME,
        result720p.s3Key,
        result720p.outputPath,
        'video/mp4',
      ),
      uploadToS3(
        ENV.AWS_S3_BUCKET_NAME,
        result480p.s3Key,
        result480p.outputPath,
        'video/mp4',
      ),
      uploadToS3(
        ENV.AWS_S3_BUCKET_NAME,
        resultThumbnail.s3Key,
        resultThumbnail.outputPath,
        'image/jpeg',
      ),
    ];

    await Promise.all(uploadTasks);
    console.log(`[Handler:${videoId}] S3 uploads complete.`);

    dbStatusUpdate = {
      status: 'READY',
      processedAt: new Date(),
      s3Key720p: result720p.s3Key,
      s3Key480p: result480p.s3Key,
      s3KeyThumbnail: resultThumbnail.s3Key,
    };

    console.log(`[Handler:${videoId}] Processing successful.`);
    return true;
  } catch (error: any) {
    console.error(
      `[Handler:${videoId}] ERROR during processing:`,
      error.message || error,
    );
    console.error(error.stack);
    processingError = error;

    dbStatusUpdate = { status: 'ERROR' };

    return false;
  } finally {
    if (dbStatusUpdate) {
      console.log(
        `[Handler:${videoId}] Updating final DB status to ${dbStatusUpdate.status}...`,
      );
      try {
        await db
          .update(videos)
          .set(dbStatusUpdate)
          .where(eq(videos.id, videoId));
        console.log(
          `[Handler:${videoId}] Final DB status updated successfully.`,
        );

        if (!processingError && dbStatusUpdate.status === 'READY') {
          console.log(`[Handler:${videoId}] Publishing SUCCESS event...`);
          await videoEventProducer.publishVideoEvent(
            VIDEO_PROCESSING_COMPLETED_ROUTING_KEY,
            {
              videoId: videoId,
              status: 'READY',
              keys: {
                s3Key720p: dbStatusUpdate.s3Key720p,
                s3Key480p: dbStatusUpdate.s3Key480p,
                s3KeyThumbnail: dbStatusUpdate.s3KeyThumbnail,
              },
            },
          );
        } else if (processingError) {
          console.log(`[Handler:${videoId}] Publishing FAILURE event...`);
          await videoEventProducer.publishVideoEvent(
            VIDEO_PROCESSING_FAILED_ROUTING_KEY,
            {
              videoId: videoId,
              status: 'ERROR',
              error: processingError.message || 'Unknown processing error',
              originalS3Key: originalS3Key,
            },
          );
        }
      } catch (dbError: any) {
        console.error(
          `[Handler:${videoId}] FAILED to update final DB status to ${dbStatusUpdate.status}:`,
          dbError.message,
        );
      }
    } else {
      console.warn(
        `[Handler:${videoId}] No final DB status update was prepared. This might indicate an early error before processing status was set.`,
      );
      if (processingError) {
        await videoEventProducer.publishVideoEvent(
          VIDEO_PROCESSING_FAILED_ROUTING_KEY,
          {
            videoId: videoId,
            status: 'ERROR',
            error: `Early failure: ${processingError.message || 'Unknown processing error'}`,
            originalS3Key: originalS3Key,
          },
        );
      }
    }

    console.log(
      `[Handler:${videoId}] Cleaning up temporary directory: ${jobTempDir}`,
    );
    await rm(jobTempDir, { recursive: true, force: true }).catch((err) => {
      console.error(
        `[Handler:${videoId}] Error cleaning up temp directory ${jobTempDir}:`,
        err,
      );
    });

    console.log(`[Handler:${videoId}] Finished job.`);
  }
};
