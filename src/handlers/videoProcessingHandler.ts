import { ConsumeMessage } from 'amqplib';
import { eq } from 'drizzle-orm';
import { mkdir, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

import { VideoUploadPayload } from '../consumers/VideoProcessingConsumer';
import { db } from '../db';
import {
  videos,
  VideoStatus,
  ProcessedFiles,
  VideoMetadata,
} from '../db/schema';
import { ENV } from '../config/env';
import { logger } from '../config/logger';
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
  getVideoMetadata,
  VideoMetadataProbe,
} from '../utils/ffmpeg.util';
import { videoEventProducer } from '../producers/videoEvent.producer';

const TEMP_BASE_DIR = join(tmpdir(), 'video-processing-service');

export const handleVideoUploadEvent = async (
  payload: VideoUploadPayload,
  msg: ConsumeMessage,
): Promise<boolean> => {
  const { videoId, s3Key: originalS3Key, originalname, mimetype } = payload;
  const logCtx = {
    videoId,
    originalS3Key,
    originalname,
    deliveryTag: msg.fields.deliveryTag,
  };
  logger.info(logCtx, 'Received video upload event');

  if (!videoId || !originalS3Key || !originalname || !mimetype) {
    logger.error(
      { ...logCtx, payload },
      'Invalid message payload received. Missing required fields.',
    );
    return false;
  }

  const safeOriginalFilename = originalname.replace(/[^a-zA-Z0-9._-]/g, '_');
  const jobTempDir = join(TEMP_BASE_DIR, videoId);
  const localOriginalPath = join(
    jobTempDir,
    `original_${safeOriginalFilename}`,
  );

  let isSuccess = false;
  let processingError: Error | null = null;
  let dbUpdateData: Partial<typeof videos.$inferInsert> = {};
  let extractedMetadata: VideoMetadata = null;
  let processedFilesResult: ProcessedFiles = {};

  try {
    logger.debug(logCtx, 'Checking video status in database');
    const existingVideo = await db.query.videos.findFirst({
      columns: { status: true },
      where: eq(videos.id, videoId),
    });

    if (!existingVideo) {
      logger.error(
        logCtx,
        'Video record not found in DB. Cannot process. NACKing message.',
      );

      await videoEventProducer.publishVideoEvent(
        VIDEO_PROCESSING_FAILED_ROUTING_KEY,
        {
          videoId: videoId,
          status: 'ERROR',
          error: { message: 'Video record not found in database' },
          originalS3Key,
        },
      );
      return false;
    }

    if (
      existingVideo.status &&
      ['PROCESSING', 'READY', 'ERROR'].includes(existingVideo.status)
    ) {
      logger.warn(
        { ...logCtx, status: existingVideo.status },
        'Video already processed or is processing. Skipping duplicate message. ACK PENDING.',
      );
      return true;
    }

    logger.info(logCtx, `Processing job. Temp dir: ${jobTempDir}`);
    await mkdir(jobTempDir, { recursive: true });
    logger.debug(logCtx, 'Created temp directory.');

    logger.info(logCtx, 'Setting video status to PROCESSING');
    await db
      .update(videos)
      .set({ status: 'PROCESSING', updatedAt: new Date() })
      .where(eq(videos.id, videoId));
    dbUpdateData.status = 'PROCESSING';

    logger.info(logCtx, 'Downloading original video from S3');
    await downloadFromS3(
      ENV.AWS_S3_BUCKET_NAME,
      originalS3Key,
      localOriginalPath,
    );
    logger.info(logCtx, 'Original video downloaded.');

    try {
      logger.info(logCtx, 'Extracting video metadata using ffprobe');
      const probeData: VideoMetadataProbe = await getVideoMetadata(
        localOriginalPath,
        videoId,
      );
      logger.info(logCtx, 'Metadata extracted');

      const videoStream = probeData.streams.find(
        (s) => s.codec_type === 'video',
      );
      extractedMetadata = {
        durationSeconds: probeData.format.duration ?? null,
        width: videoStream?.width ?? null,
        height: videoStream?.height ?? null,
        formatName: probeData.format.format_name ?? null,
        bitRate: probeData.format.bit_rate ?? null,
      };

      dbUpdateData.metadata = extractedMetadata;
      logger.debug(
        { ...logCtx, metadata: extractedMetadata },
        'Prepared metadata for DB update',
      );
    } catch (metaError: any) {
      logger.warn(
        { ...logCtx, error: metaError.message },
        'Failed to extract video metadata, continuing process without it.',
      );

      dbUpdateData.metadata = null;
    }

    logger.info(logCtx, 'Starting parallel FFmpeg processing tasks...');
    const targetResolutions = ENV.PROCESSING_RESOLUTIONS;
    const processingPromises: Array<
      Promise<TranscodeResult | ThumbnailResult>
    > = [];

    targetResolutions.forEach((height) => {
      const s3Key = `${S3_PROCESSED_PREFIX}${videoId}/${videoId}_${height}p.mp4`;
      logger.debug({ ...logCtx, height, s3Key }, 'Queueing transcode task');
      processingPromises.push(
        transcodeToResolution(
          localOriginalPath,
          jobTempDir,
          height,
          videoId,
          s3Key,
        ),
      );
    });

    const thumbnailS3Key = `${S3_THUMBNAIL_PREFIX}${videoId}/${videoId}_thumbnail.jpg`;
    logger.debug(
      { ...logCtx, s3Key: thumbnailS3Key },
      'Queueing thumbnail task',
    );
    processingPromises.push(
      generateThumbnail(localOriginalPath, jobTempDir, videoId, thumbnailS3Key),
    );

    const processingResults = await Promise.all(processingPromises);
    logger.info(logCtx, 'FFmpeg processing tasks completed.');

    const transcodeResults = processingResults.filter(
      (r) => 'resolution' in r,
    ) as TranscodeResult[];
    const thumbnailResult = processingResults.find(
      (r) => !('resolution' in r),
    ) as ThumbnailResult | undefined;

    processedFilesResult = {};
    if (thumbnailResult) {
      processedFilesResult.thumbnail = thumbnailResult.s3Key;
    }
    transcodeResults.forEach((result) => {
      processedFilesResult![result.resolution] = result.s3Key;
    });
    dbUpdateData.processedFiles = processedFilesResult;
    logger.debug(
      { ...logCtx, files: processedFilesResult },
      'Prepared processed file keys',
    );

    logger.info(logCtx, 'Starting parallel S3 uploads for processed files...');
    const uploadPromises: Promise<string | undefined>[] = [];

    transcodeResults.forEach((result) => {
      logger.debug(
        { ...logCtx, path: result.outputPath, key: result.s3Key },
        'Queueing S3 upload task (video)',
      );
      uploadPromises.push(
        uploadToS3(
          ENV.AWS_S3_BUCKET_NAME,
          result.s3Key,
          result.outputPath,
          'video/mp4',
        ),
      );
    });

    if (thumbnailResult) {
      logger.debug(
        {
          ...logCtx,
          path: thumbnailResult.outputPath,
          key: thumbnailResult.s3Key,
        },
        'Queueing S3 upload task (thumbnail)',
      );
      uploadPromises.push(
        uploadToS3(
          ENV.AWS_S3_BUCKET_NAME,
          thumbnailResult.s3Key,
          thumbnailResult.outputPath,
          'image/jpeg',
        ),
      );
    }

    await Promise.all(uploadPromises);
    logger.info(logCtx, 'S3 uploads completed.');

    dbUpdateData.status = 'READY';
    dbUpdateData.processedAt = new Date();
    isSuccess = true;
    logger.info(logCtx, 'Video processing successful.');
  } catch (error: any) {
    processingError = error;
    logger.error(
      { ...logCtx, err: error.message, stack: error.stack },
      'ERROR during video processing',
    );
    dbUpdateData.status = 'ERROR';
    dbUpdateData.processedFiles = processedFilesResult;
    isSuccess = false;
  } finally {
    logger.debug(
      logCtx,
      'Entering finally block for cleanup and final updates',
    );
    try {
      if (Object.keys(dbUpdateData).length > 0 && dbUpdateData.status) {
        logger.info(
          { ...logCtx, status: dbUpdateData.status },
          `Updating final DB status to ${dbUpdateData.status}`,
        );

        dbUpdateData.updatedAt = new Date();

        await db.update(videos).set(dbUpdateData).where(eq(videos.id, videoId));
        logger.info(logCtx, 'Final DB status updated successfully.');
      } else {
        logger.warn(
          logCtx,
          'No final DB status update was performed (likely early exit or no status change needed).',
        );
      }

      if (isSuccess && dbUpdateData.status === 'READY') {
        logger.info(logCtx, 'Publishing video processing completion event');
        await videoEventProducer.publishVideoEvent(
          VIDEO_PROCESSING_COMPLETED_ROUTING_KEY,
          {
            videoId: videoId,
            status: 'READY',
            outputs: dbUpdateData.processedFiles || {},
            metadata: dbUpdateData.metadata || {},
          },
        );
      } else if (processingError) {
        logger.info(logCtx, 'Publishing video processing failure event');
        await videoEventProducer.publishVideoEvent(
          VIDEO_PROCESSING_FAILED_ROUTING_KEY,
          {
            videoId: videoId,
            status: 'ERROR',
            error: {
              message: processingError.message || 'Unknown processing error',
            },
            originalS3Key: originalS3Key,
          },
        );
      }
    } catch (finalError: any) {
      logger.error(
        { ...logCtx, err: finalError.message, stack: finalError.stack },
        `CRITICAL: Failed during final DB update or event publishing after processing attempt (status: ${dbUpdateData.status})`,
      );
    }

    logger.info(logCtx, `Cleaning up temporary directory: ${jobTempDir}`);
    await rm(jobTempDir, { recursive: true, force: true }).catch((err) => {
      logger.error(
        { ...logCtx, err, path: jobTempDir },
        'Error cleaning up temp directory',
      );
    });

    logger.info(logCtx, `Finished processing job.`);
  }

  return isSuccess;
};
