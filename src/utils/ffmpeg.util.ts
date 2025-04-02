import Ffmpeg from 'fluent-ffmpeg';
import path, { join } from 'node:path';
import { mkdir } from 'node:fs/promises';

import { logger } from '../config/logger';
import { ENV } from '../config/env';

export interface VideoMetadataProbe {
  format: {
    filename?: string;
    nb_streams?: number;
    format_name?: string;
    format_long_name?: string;
    start_time?: number;
    duration?: number;
    size?: number;
    bit_rate?: string;
    probe_score?: number;
    tags?: Record<string, string>;
  };
  streams: Array<{
    index: number;
    codec_name?: string;
    codec_long_name?: string;
    profile?: string;
    codec_type?: 'video' | 'audio' | 'subtitle' | 'data';
    codec_tag_string?: string;
    codec_tag?: string;
    width?: number;
    height?: number;
    coded_width?: number;
    coded_height?: number;
    closed_captions?: number;
    film_grain?: number;
    has_b_frames?: number;
    sample_aspect_ratio?: string;
    display_aspect_ratio?: string;
    pix_fmt?: string;
    level?: number;
    color_range?: string;
    color_space?: string;
    color_transfer?: string;
    color_primaries?: string;
    chroma_location?: string;
    field_order?: string;
    refs?: number;
    is_avc?: string;
    nal_length_size?: string;
    id?: string;
    r_frame_rate?: string; // e.g., "30/1"
    avg_frame_rate?: string; // e.g., "30/1"
    time_base?: string; // e.g., "1/15360"
    start_pts?: number;
    start_time?: number;
    duration_ts?: number;
    duration?: number;
    bit_rate?: string;
    bits_per_raw_sample?: string;
    nb_frames?: string;
    extradata_size?: number;
    // Audio specific
    sample_fmt?: string;
    sample_rate?: string;
    channels?: number;
    channel_layout?: string;
    bits_per_sample?: number;
    // Tags
    tags?: {
      language?: string;
      handler_name?: string;
      vendor_id?: string;
      encoder?: string;
    };
    disposition?: Record<string, number>;
  }>;
}

export interface TranscodeResult {
  outputPath: string;
  resolution: number;
  s3Key: string;
}

export interface ThumbnailResult {
  outputPath: string;
  s3Key: string;
}

/**
 * Extracts metadata from a video file using ffprobe.
 * @param inputPath Path to the source video file.
 * @param videoId Unique ID for logging context.
 * @returns Promise resolving with the extracted metadata.
 */
export function getVideoMetadata(
  inputPath: string,
  videoId: string,
): Promise<VideoMetadataProbe> {
  const logCtx = { videoId, inputPath };
  return new Promise((resolve, reject) => {
    logger.debug(logCtx, '[FFPROBE] Starting metadata extraction');
    Ffmpeg.ffprobe(inputPath, (err, metadata) => {
      if (err) {
        logger.error({ ...logCtx, err }, '[FFPROBE] Error extracting metadata');
        return reject(new Error(`ffprobe failed: ${err.message}`));
      }

      logger.info(logCtx, '[FFPROBE] Metadata extraction successful');
      logger.debug(
        {
          ...logCtx,
          format: metadata.format,
          streamCount: metadata.streams?.length,
        },
        '[FFPROBE] Extracted Metadata',
      );
      resolve(metadata);
    });
  });
}

/**
 * Transcodes a video file to a specified height, maintaining aspect ratio.
 * @param inputPath Path to the source video file.
 * @param outputDir Directory to save the transcoded file temporarily.
 * @param targetHeight The target vertical resolution (e.g., 720, 480).
 * @param videoId The unique ID of the video for naming and logging.
 * @param targetS3Key The intended final S3 key for this output.
 * @returns Promise resolving with the path to the output file and resolution info.
 */
export function transcodeToResolution(
  inputPath: string,
  outputDir: string,
  targetHeight: number,
  videoId: string,
  targetS3Key: string,
): Promise<TranscodeResult> {
  const logCtx = { videoId, targetHeight, inputPath, targetS3Key };
  return new Promise(async (resolve, reject) => {
    const baseFilename = `${videoId}_${targetHeight}p.mp4`;
    const outputPath = join(outputDir, baseFilename);

    const scaleFilter = `scale=-2:${targetHeight}`;

    try {
      await mkdir(outputDir, { recursive: true });

      logger.info(
        logCtx,
        `[FFMPEG] Starting transcode (${targetHeight}p): -> ${outputPath}`,
      );

      let command = Ffmpeg(inputPath)
        .output(outputPath)
        .videoCodec('libx264')
        .audioCodec('aac')
        .outputOptions(['-preset medium', '-crf 23', '-movflags +faststart'])
        .videoFilter(scaleFilter);
      // .size(targetSize)

      command
        .on('start', (commandLine) => {
          logger.debug(
            logCtx,
            `[FFMPEG] Spawned Ffmpeg (${targetHeight}p) command: ${commandLine.substring(0, 200)}...`,
          );
        })
        .on('progress', (progress) => {
          if (progress.percent && Math.round(progress.percent) % 10 === 0) {
            logger.debug(
              logCtx,
              `[FFMPEG] Processing ${baseFilename}: ${progress.percent.toFixed(1)}% done`,
            );
          }
        })
        .on('end', (stdout, stderr) => {
          logger.info(
            logCtx,
            `[FFMPEG] Transcoding (${targetHeight}p) finished successfully: ${outputPath}`,
          );

          if (stderr) {
            logger.debug(
              { ...logCtx, stderr },
              `[FFMPEG] stderr output for ${targetHeight}p (success)`,
            );
          }
          resolve({
            outputPath,
            resolution: targetHeight,
            s3Key: targetS3Key,
          });
        })
        .on('error', (err, stdout, stderr) => {
          logger.error(
            { ...logCtx, err: err.message, stdout, stderr },
            `[FFMPEG] Error during transcoding (${targetHeight}p) for ${outputPath}`,
          );
          reject(
            new Error(
              `FFmpeg transcoding (${targetHeight}p) failed: ${err.message}`,
            ),
          );
        })
        .run();
    } catch (error) {
      logger.error(
        { ...logCtx, error },
        `[FFMPEG] Pre-execution error for transcoding (${targetHeight}p)`,
      );
      reject(error);
    }
  });
}

/**
 * Generates a thumbnail from a video file using environment config.
 * @param inputPath Path to the source video file.
 * @param outputDir Directory to save the thumbnail file temporarily.
 * @param videoId The unique ID of the video for naming and logging.
 * @param targetS3Key The intended final S3 key for the thumbnail.
 * @returns Promise resolving with the path to the generated thumbnail.
 */
export function generateThumbnail(
  inputPath: string,
  outputDir: string,
  videoId: string,
  targetS3Key: string,
): Promise<ThumbnailResult> {
  const timeMark = ENV.THUMBNAIL_TIMEMARK;
  const size = ENV.THUMBNAIL_SIZE;
  const logCtx = { videoId, inputPath, targetS3Key, timeMark, size };

  return new Promise(async (resolve, reject) => {
    const thumbnailFilename = `${videoId}_thumbnail.jpg`;
    const outputPath = path.join(outputDir, thumbnailFilename);

    try {
      await mkdir(outputDir, { recursive: true });

      logger.info(
        logCtx,
        `[FFMPEG] Generating thumbnail: -> ${outputPath} at ${timeMark}`,
      );

      Ffmpeg(inputPath)
        .screenshots({
          timestamps: [timeMark],
          filename: thumbnailFilename,
          folder: outputDir,
          size: size,
        })
        .on('end', (stdout, stderr) => {
          logger.info(
            logCtx,
            `[FFMPEG] Thumbnail generated successfully: ${outputPath}`,
          );
          if (stderr) {
            logger.debug(
              { ...logCtx, stderr },
              `[FFMPEG] stderr output for thumbnail (success)`,
            );
          }
          resolve({ outputPath, s3Key: targetS3Key });
        })
        .on('error', (err, stdout, stderr) => {
          logger.error(
            { ...logCtx, err: err.message, stdout, stderr },
            `[FFMPEG] Error generating thumbnail for ${outputPath}`,
          );
          reject(
            new Error(`FFmpeg thumbnail generation failed: ${err.message}`),
          );
        });
    } catch (error) {
      logger.error(
        { ...logCtx, error },
        `[FFMPEG] Pre-execution error for thumbnail generation`,
      );
      reject(error);
    }
  });
}
