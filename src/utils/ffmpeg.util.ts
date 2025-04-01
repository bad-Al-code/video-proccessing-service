import Ffmpeg from 'fluent-ffmpeg';
import path, { join } from 'node:path';
import { mkdir } from 'node:fs/promises';

import { logger } from '../config/logger';

export interface TranscodeResult {
  outputPath: string;
  resolution: string;
  s3Key: string;
}

export interface ThumbnailResult {
  outputPath: string;
  s3Key: string;
}

/**
 * Transcodes a video file to a specified resolution (e.g., 720p, 480p).
 * @param inputPath Path to the source video file.
 * @param outputDir Directory to save the transcoded file temporarily.
 * @param targetResolution String identifier like '720p' or '480p'.
 * @param videoId The unique ID of the video for naming.
 * @param targetS3Key The intended final S3 key for this output.
 * @returns Promise resolving with the path to the output file and resolution info.
 */
export function transcodeToResolution(
  inputPath: string,
  outputDir: string,
  targetResolution: '720p' | '480p',
  videoId: string,
  targetS3Key: string,
): Promise<TranscodeResult> {
  return new Promise(async (resolve, reject) => {
    const baseFilename = `${videoId}_${targetResolution}.mp4`;
    const outputPath = join(outputDir, baseFilename);

    const resolutionMap: Record<string, string> = {
      '720p': '1280x720',
      '480p': '854x480',
    };
    const targetSize = resolutionMap[targetResolution];
    if (!targetSize) {
      return reject(
        new Error(`Invalid target resolution specified: ${targetResolution}`),
      );
    }

    try {
      await mkdir(outputDir, { recursive: true });

      logger.info(
        `[FFMPEG:${videoId}] Starting transcode (${targetResolution}): ${inputPath} -> ${outputPath}`,
      );

      Ffmpeg(inputPath)
        .output(outputPath)
        .videoCodec('libx264')
        .audioCodec('aac')
        .size(targetSize)
        .on('start', (commandLine) => {
          logger.info(
            `[FFMPEG:${videoId}] Spawned Ffmpeg (${targetResolution}) command: ${commandLine}`,
          );
        })
        .on('progress', (progress) => {
          if (progress.percent) {
            logger.info(
              `[FFMPEG:${videoId}] Processing ${baseFilename}: ${progress.percent.toFixed(2)}% done`,
            );
          }
        })
        .on('end', (stdout, stderr) => {
          logger.info(
            `[FFMPEG:${videoId}] Transcoding (${targetResolution}) finished successfully: ${outputPath}`,
          );
          resolve({
            outputPath,
            resolution: targetResolution,
            s3Key: targetS3Key,
          });
        })
        .on('error', (err, stdout, stderr) => {
          logger.error(
            `[FFMPEG:${videoId}] Error during transcoding (${targetResolution}) for ${outputPath}:`,
            err.message,
          );
          logger.error(`[FFMPEG:${videoId}] stderr:`, stderr);
          reject(
            new Error(
              `FFmpeg transcoding (${targetResolution}) failed: ${err.message}`,
            ),
          );
        })
        .run();
    } catch (error) {
      reject(error);
    }
  });
}

/**
 * Generates a thumbnail from a video file.
 * @param inputPath Path to the source video file.
 * @param outputDir Directory to save the thumbnail file temporarily.
 * @param videoId The unique ID of the video for naming.
 * @param targetS3Key The intended final S3 key for the thumbnail.
 * @param timeMark Time mark for the screenshot (e.g., '10%', '00:00:05'). Defaults to '10%'.
 * @returns Promise resolving with the path to the generated thumbnail.
 */
export function generateThumbnail(
  inputPath: string,
  outputDir: string,
  videoId: string,
  targetS3Key: string,
  timeMark: string = '10%',
): Promise<ThumbnailResult> {
  return new Promise(async (resolve, reject) => {
    const thumbnailFilename = `${videoId}_thumbnail.jpg`;
    const outputPath = path.join(outputDir, thumbnailFilename);

    try {
      await mkdir(outputDir, { recursive: true });

      logger.info(
        `[FFMPEG:${videoId}] Generating thumbnail: ${inputPath} -> ${outputPath} at ${timeMark}`,
      );

      Ffmpeg(inputPath)
        .screenshots({
          timestamps: [timeMark],
          filename: thumbnailFilename,
          folder: outputDir,
          size: '640x?',
        })
        .on('end', () => {
          logger.info(
            `[FFMPEG:${videoId}] Thumbnail generated successfully: ${outputPath}`,
          );
          resolve({ outputPath, s3Key: targetS3Key });
        })
        .on('error', (err) => {
          logger.error(
            `[FFMPEG:${videoId}] Error generating thumbnail for ${outputPath}:`,
            err.message,
          );
          reject(
            new Error(`FFmpeg thumbnail generation failed: ${err.message}`),
          );
        });
    } catch (error) {
      reject(error);
    }
  });
}
