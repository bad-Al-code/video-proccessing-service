import Ffmpeg, { FfprobeData } from 'fluent-ffmpeg';
import path, { join } from 'node:path';
import { mkdir } from 'node:fs/promises';

import { logger } from '../config/logger';
import { ENV, ProcessingResolution } from '../config/env';

export interface VideoMetadata {
  durationSeconds?: number;
  width?: number;
  height?: number;
}

export interface TranscodeResult {
  outputPath: string;
  resolution: string;
  s3Key: string;
}

export interface ThumbnailResult {
  outputPath: string;
  s3Key: string;
  timestamp: string;
}

export interface HlsResult {
  outputDir: string;
  manifestFilename: string;
  s3ManifestKey: string;
  s3Prefix: string;
}

const resolutionDetails: Record<string, { width: number; height: number }> = {
  '1080p': { width: 1920, height: 1080 },
  '720p': { width: 1280, height: 720 },
  '480p': { width: 854, height: 480 },
  '360p': { width: 640, height: 360 },
};

function getWatermarkFilterString(
  watermarkLocalPath: string,
  position: typeof ENV.WATERMARK_POSITION,
): string {
  const logMeta = { watermarkPath: watermarkLocalPath, position };
  logger.debug(logMeta, '[FFMPEG] Generating watermark filter string...');

  const margin = 10; // Pixels margin from the edge
  let overlayPositionCoordinates = '';

  switch (position) {
    case 'top_left':
      overlayPositionCoordinates = `x=${margin}:y=${margin}`;
      break;
    case 'top_right':
      overlayPositionCoordinates = `x=W-w-${margin}:y=${margin}`;
      break;
    case 'bottom_left':
      overlayPositionCoordinates = `x=${margin}:y=H-h-${margin}`;
      break;
    case 'center':
      overlayPositionCoordinates = `x=(W-w)/2:y=(H-h)/2`;
      break;
    case 'bottom_right':
    default:
      overlayPositionCoordinates = `x=W-w-${margin}:y=H-h-${margin}`;
      break;
  }

  const safeWatermarkPath = watermarkLocalPath.replace(/\\/g, '/');

  const filter = `movie='${safeWatermarkPath}' [watermark]; [in][watermark] overlay=${overlayPositionCoordinates} [out]`;
  logger.debug({ ...logMeta, filter }, '[FFMPEG] Generated watermark filter');
  return filter;
}

export function getVideoMetadata(
  inputPath: string,
  logContext: object = {},
): Promise<VideoMetadata> {
  const probeLogContext = { ...logContext, operation: 'metadata', inputPath };
  logger.info(probeLogContext, '[FFPROBE] Probing video file...');

  return new Promise((resolve, reject) => {
    Ffmpeg.ffprobe(inputPath, (err, metadata: FfprobeData) => {
      if (err) {
        logger.error(
          { ...probeLogContext, err },
          '[FFPROBE] Failed to probe video',
        );
        return reject(
          new Error(`ffprobe failed for ${inputPath}: ${err.message}`),
        );
      }

      const videoStream = metadata.streams.find(
        (s) => s.codec_type === 'video',
      );
      const duration = metadata.format.duration;

      const result: VideoMetadata = {
        durationSeconds:
          duration !== undefined ? Math.round(duration) : undefined,
        width: videoStream?.width,
        height: videoStream?.height,
      };

      logger.info(
        { ...probeLogContext, metadata: result },
        '[FFPROBE] Video metadata extracted.',
      );
      resolve(result);
    });
  });
}

export function transcodeToResolution(
  inputPath: string,
  outputDir: string,
  targetResolution: ProcessingResolution,
  videoId: string,
  targetS3Key: string,
  watermarkLocalPath?: string,
  logContext: object = {},
): Promise<TranscodeResult> {
  const transcodeLogContext = {
    ...logContext,
    operation: 'transcode',
    videoId,
    resolution: targetResolution,
    inputPath,
  };
  logger.info(transcodeLogContext, `[FFMPEG] Starting transcode task...`);

  return new Promise(async (resolve, reject) => {
    const resolution = resolutionDetails[targetResolution];
    if (!resolution) {
      const error = new Error(
        `Invalid target resolution specified: ${targetResolution}`,
      );
      logger.error(transcodeLogContext, error.message);
      return reject(error);
    }

    const baseFilename = `${videoId}_${targetResolution}.mp4`;
    const outputPath = join(outputDir, baseFilename);
    const targetSize = `${resolution.width}x${resolution.height}`;

    try {
      await mkdir(outputDir, { recursive: true });
      logger.debug(
        { ...transcodeLogContext, outputPath },
        `[FFMPEG] Output path prepared.`,
      );

      const command = Ffmpeg(inputPath)
        .output(outputPath)
        .videoCodec('libx264')
        .audioCodec('aac')
        .audioBitrate('128k')
        .videoBitrate(
          targetResolution === '1080p'
            ? '2500k'
            : targetResolution === '720p'
              ? '1500k'
              : '800k',
        )
        .size(targetSize)
        .outputOptions(['-preset medium', '-crf 23', '-movflags +faststart']);

      if (watermarkLocalPath) {
        logger.info(transcodeLogContext, '[FFMPEG] Applying watermark...');
        const filter = getWatermarkFilterString(
          watermarkLocalPath,
          ENV.WATERMARK_POSITION,
        );
        command.complexFilter(filter, 'out');
      }

      command
        .on('start', (commandLine) => {
          logger.debug(
            transcodeLogContext,
            `[FFMPEG] Spawned command: ${commandLine}`,
          );
        })
        .on('progress', (progress) => {
          if (progress.percent && Math.floor(progress.percent) % 20 === 0) {
            logger.debug(
              { ...transcodeLogContext, percent: progress.percent.toFixed(1) },
              `[FFMPEG] Progress`,
            );
          }
        })
        .on('end', (stdout, stderr) => {
          if (stderr) {
            logger.warn(
              {
                ...transcodeLogContext,
                stderrPreview: stderr.substring(0, 300),
              },
              `[FFMPEG] Finished with potential warnings.`,
            );
          }
          logger.info(
            { ...transcodeLogContext, outputPath },
            `[FFMPEG] Transcoding finished successfully.`,
          );
          resolve({
            outputPath,
            resolution: targetResolution,
            s3Key: targetS3Key,
          });
        })
        .on('error', (err, stdout, stderr) => {
          logger.error(
            { ...transcodeLogContext, err: err.message, stderr },
            `[FFMPEG] Error during transcoding`,
          );
          reject(
            new Error(
              `FFmpeg transcoding (${targetResolution}) failed: ${err.message}`,
            ),
          );
        })
        .run();
    } catch (error) {
      logger.error(
        { ...transcodeLogContext, error },
        `[FFMPEG] Setup error for transcode task`,
      );
      reject(error);
    }
  });
}

export function generateThumbnails(
  inputPath: string,
  outputDir: string,
  videoId: string,
  s3KeyPrefix: string,
  timestamps: string[] = ENV.THUMBNAIL_TIMESTAMPS,
  size: string = ENV.THUMBNAIL_SIZE,
  logContext: object = {},
): Promise<ThumbnailResult[]> {
  const thumbLogContext = {
    ...logContext,
    operation: 'thumbnails',
    videoId,
    timestamps,
    size,
    inputPath,
  };
  logger.info(
    thumbLogContext,
    `[FFMPEG] Starting thumbnail generation task...`,
  );

  return new Promise(async (resolve, reject) => {
    if (!timestamps || timestamps.length === 0) {
      logger.warn(
        thumbLogContext,
        '[FFMPEG] No thumbnail timestamps configured. Skipping generation.',
      );
      return resolve([]);
    }

    const outputFilenamePattern = `${videoId}_thumb_%i.jpg`;
    const fullOutputPathPattern = path.join(outputDir, outputFilenamePattern);

    try {
      await mkdir(outputDir, { recursive: true });
      logger.debug(
        { ...thumbLogContext, outputDir },
        `[FFMPEG] Thumbnail output directory prepared.`,
      );

      Ffmpeg(inputPath)
        .screenshots({
          timestamps: timestamps,
          filename: outputFilenamePattern,
          folder: outputDir,
          size: size,
        })
        .on('start', (commandLine) => {
          logger.debug(
            thumbLogContext,
            `[FFMPEG] Spawned command: ${commandLine}`,
          );
        })
        .on('end', () => {
          logger.info(
            thumbLogContext,
            `[FFMPEG] Thumbnail generation finished successfully.`,
          );
          const results: ThumbnailResult[] = timestamps.map((ts, index) => {
            const filename = outputFilenamePattern.replace(
              '%i',
              String(index + 1),
            );
            const outputPath = path.join(outputDir, filename);
            return {
              outputPath: outputPath,
              s3Key: `${s3KeyPrefix}${filename}`,
              timestamp: ts,
            };
          });
          resolve(results);
        })
        .on('error', (err) => {
          logger.error(
            { ...thumbLogContext, err: err.message },
            `[FFMPEG] Error generating thumbnails`,
          );
          reject(
            new Error(`FFmpeg thumbnail generation failed: ${err.message}`),
          );
        });
    } catch (error) {
      logger.error(
        { ...thumbLogContext, error },
        `[FFMPEG] Setup error for thumbnail generation`,
      );
      reject(error);
    }
  });
}

export function transcodeToHLS(
  inputPath: string,
  outputDir: string,
  targetResolution: ProcessingResolution = ENV.HLS_RESOLUTION,
  videoId: string,
  targetS3ManifestKey: string,
  targetS3Prefix: string,
  watermarkLocalPath?: string,
  logContext: object = {},
): Promise<HlsResult> {
  const hlsLogContext = {
    ...logContext,
    operation: 'hls',
    videoId,
    resolution: targetResolution,
    inputPath,
  };
  logger.info(hlsLogContext, `[FFMPEG] Starting HLS transcoding task...`);

  return new Promise(async (resolve, reject) => {
    const resolution = resolutionDetails[targetResolution];
    if (!resolution) {
      const error = new Error(
        `Invalid target resolution for HLS: ${targetResolution}`,
      );
      logger.error(hlsLogContext, error.message);
      return reject(error);
    }
    const targetSize = `${resolution.width}x${resolution.height}`;

    const hlsOutputDir = join(outputDir, `hls_${targetResolution}`);
    const manifestFilename = `master.m3u8`;
    const segmentFilenamePattern = `${videoId}_${targetResolution}_%05d.ts`;
    const localManifestPath = join(hlsOutputDir, manifestFilename);
    const localSegmentPathPattern = join(hlsOutputDir, segmentFilenamePattern);

    try {
      await mkdir(hlsOutputDir, { recursive: true });
      logger.debug(
        { ...hlsLogContext, hlsOutputDir },
        `[FFMPEG] HLS output directory prepared.`,
      );

      const command = Ffmpeg(inputPath)
        .videoCodec('libx264')
        .audioCodec('aac')
        .audioBitrate('96k')
        .videoBitrate(
          targetResolution === '720p'
            ? '1200k'
            : targetResolution === '480p'
              ? '700k'
              : '400k',
        )
        .size(targetSize)
        .outputOptions([
          '-preset medium',
          '-crf 24',
          '-profile:v main',
          '-f hls',
          '-hls_time 6',
          '-hls_list_size 0',
          `-hls_segment_filename ${localSegmentPathPattern}`,
          '-start_number 0',
        ])
        .output(localManifestPath);

      if (watermarkLocalPath) {
        logger.info(hlsLogContext, '[FFMPEG] Applying watermark to HLS...');
        const filter = getWatermarkFilterString(
          watermarkLocalPath,
          ENV.WATERMARK_POSITION,
        );
        command.complexFilter(filter, 'out');
      }

      command
        .on('start', (commandLine) => {
          logger.debug(
            hlsLogContext,
            `[FFMPEG] Spawned HLS command: ${commandLine}`,
          );
        })
        .on('progress', (progress) => {
          if (progress.percent && Math.floor(progress.percent) % 25 === 0) {
            logger.debug(
              { ...hlsLogContext, percent: progress.percent.toFixed(1) },
              `[FFMPEG] HLS Progress`,
            );
          }
        })
        .on('end', (stdout, stderr) => {
          if (stderr) {
            logger.warn(
              { ...hlsLogContext, stderrPreview: stderr.substring(0, 300) },
              `[FFMPEG] HLS finished with potential warnings.`,
            );
          }
          logger.info(
            { ...hlsLogContext, hlsOutputDir },
            `[FFMPEG] HLS transcoding finished successfully.`,
          );
          resolve({
            outputDir: hlsOutputDir,
            manifestFilename: manifestFilename,
            s3ManifestKey: targetS3ManifestKey,
            s3Prefix: targetS3Prefix,
          });
        })
        .on('error', (err, stdout, stderr) => {
          logger.error(
            { ...hlsLogContext, err: err.message, stderr },
            `[FFMPEG] Error during HLS transcoding`,
          );
          reject(
            new Error(
              `FFmpeg HLS transcoding (${targetResolution}) failed: ${err.message}`,
            ),
          );
        })
        .run();
    } catch (error) {
      logger.error(
        { ...hlsLogContext, error },
        `[FFMPEG] Setup error for HLS task`,
      );
      reject(error);
    }
  });
}
