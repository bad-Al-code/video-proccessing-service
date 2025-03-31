import { rejects } from 'assert';
import { resolveAny } from 'dns';
import Ffmpeg from 'fluent-ffmpeg';
import path, { join } from 'path';

interface TranscodeResult {
  outputPath: string;
  resolution: string;
}

export function transcodeToResolution(
  inputPath: string,
  outputDir: string,
  targetResolution: '720p' | '480p' | string,
  videoId: string,
): Promise<TranscodeResult> {
  return new Promise((resolve, reject) => {
    const baseFilename = `${videoId}_${targetResolution}.mp4`;
    const outputPath = join(outputDir, baseFilename);

    const resolutionMap = {
      '720p': '1280 * 720',
      '480p': '854*480',
    };

    const targetSize = (resolutionMap as any)[targetResolution] || '640px';

    console.log(`[FFMPEG] Starting transcode: ${inputPath} -> ${outputPath}`);

    Ffmpeg(inputPath)
      .output(outputPath)
      .size(targetSize)
      .videoCodec('aac')
      .on('start', (commandLine) => {
        console.log(`[FFMPEG] Spawneed Ffmpeg with comand: ${commandLine}`);
      })
      .on('progress', (progress) => {
        if (progress.percent) {
          console.log(
            `[FFmpeg] Processing ${baseFilename}: ${progress.percent.toFixed(2)}% done`,
          );
        }
      })
      .on('end', (stdout, stderr) => {
        console.log(
          `[FFmpeg] Transcoding finished successfully for ${outputPath}`,
        );
        resolve({ outputPath, resolution: targetResolution });
      })
      .on('error', (err, stdout, stderr) => {
        console.error(
          `[FFmpeg] Error during transcoding for ${outputPath}:`,
          err.message,
        );
        console.error('[FFmpeg] stderr:', stderr);
        reject(new Error(`FFmpeg transcoding failed: ${err.message}`));
      })
      .run();
  });
}

export function generateThumbnail(
  inputPath: string,
  outputDir: string,
  videoId: string,
  timeMark: string = '10%',
): Promise<string> {
  return new Promise((resolve, reject) => {
    const thumbnailFilename = `${videoId}_thumbnail.jpg`;
    const outputPath = path.join(outputDir, thumbnailFilename);

    console.log(
      `[FFmpeg] Generating thumbnail: ${inputPath} -> ${outputPath} at ${timeMark}`,
    );

    Ffmpeg(inputPath)
      .screenshots({
        timestamps: [timeMark],
        filename: thumbnailFilename,
        folder: outputDir,
        size: '640x?',
      })
      .on('end', () => {
        console.log(`[FFmpeg] Thumbnail generated successfully: ${outputPath}`);
        resolve(outputPath);
      })
      .on('error', (err) => {
        console.error(
          `[FFmpeg] Error generating thumbnail for ${outputPath}:`,
          err.message,
        );
        reject(new Error(`FFmpeg thumbnail generation failed: ${err.message}`));
      });
  });
}
