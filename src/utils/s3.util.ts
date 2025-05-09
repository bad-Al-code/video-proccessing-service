import { Readable } from 'node:stream';
import { createReadStream, createWriteStream } from 'node:fs';
import { unlink, stat } from 'node:fs/promises';
import {
  GetObjectCommand,
  PutObjectCommand,
  DeleteObjectCommand,
} from '@aws-sdk/client-s3';

import { s3Client } from '../config/s3Client';
import { logger } from '../config/logger';

/**
 * Downloads an object from S3 to a local file path.
 * @param bucket The S3 bucket name.
 * @param key The S3 object key.
 * @param downloadPath The local file system path to save the download.
 */
export async function downloadFromS3(
  bucket: string,
  key: string,
  downloadPath: string,
): Promise<void> {
  logger.info(
    `[S3] Attempting download: s3://${bucket}/${key} -> ${downloadPath}`,
  );
  try {
    const command = new GetObjectCommand({ Bucket: bucket, Key: key });
    const { Body, ContentLength } = await s3Client.send(command);

    if (!Body || !(Body instanceof Readable)) {
      throw new Error(`Failed to get readable stream for S3 object: ${key}`);
    }

    logger.info(
      `[S3] Downloading ${ContentLength ? (ContentLength / 1024 / 1024).toFixed(2) + ' MB' : 'unknown size'}...`,
    );
    const writer = createWriteStream(downloadPath);
    logger.info(
      `[S3:Download:${key}] Write stream created. Setting up pipe...`,
    );

    await new Promise<void>((resolve, reject) => {
      logger.info(
        `[S3:Download:${key}] Inside Promise executor. Piping Body to writer...`,
      );

      writer.on('finish', () => {
        logger.info(`[S3:Download:${key}] Writer 'finish' event received.`);
        resolve();
      });

      writer.on('error', (err) => {
        logger.error(
          `[S3:Download:${key}] Writer 'error' event received:`,
          err,
        );
        unlink(downloadPath).catch(() => {});
        reject(err);
      });

      Body.on('error', (err) => {
        writer.close();
        unlink(downloadPath).catch(() => {});
        reject(err);
      });

      let bytesDownloaded = 0;
      Body.on('data', (chunk) => {
        bytesDownloaded += chunk.length;
        logger.info(
          `[S3:Download:${key}] Received data chunk. Total: ${bytesDownloaded}`,
        );
      });

      Body.on('end', () => {
        logger.info(
          `[S3:Download:${key}] S3 Body 'end' event received. Total bytes: ${bytesDownloaded}`,
        );
      });

      Body.pipe(writer);
      logger.info(`[S3:Download:${key}] Pipe initiated.`);
    });

    logger.info(`[S3] Download complete: ${downloadPath}`);
  } catch (error: any) {
    logger.error(
      `[S3] Download failed for s3://${bucket}/${key}:`,
      error.message || error,
    );
    try {
      await stat(downloadPath);
      await unlink(downloadPath);
      logger.info(`[S3] Cleaned up partial download: ${downloadPath}`);
    } catch (cleanupError: any) {
      if (cleanupError.code !== 'ENOENT') {
        logger.error(
          `[S3] Error cleaning up partial download ${downloadPath}:`,
          cleanupError.message,
        );
      }
    }
    throw new Error(
      `[S3] Download failed for ${key}: ${error.message || error}`,
    );
  }
}

/**
 * Uploads a local file to S3.
 * @param bucket The destination S3 bucket name.
 * @param key The destination S3 object key.
 * @param filePath The path to the local file to upload.
 * @param contentType The MIME type of the file (e.g., 'video/mp4', 'image/jpeg').
 * @returns Promise resolving with the ETag of the uploaded object or undefined on failure.
 */
export async function uploadToS3(
  bucket: string,
  key: string,
  filePath: string,
  contentType: string,
): Promise<string | undefined> {
  logger.info(`[S3] Attempting upload: ${filePath} -> s3://${bucket}/${key}`);
  let fileStream;
  try {
    const fileStats = await stat(filePath);
    if (fileStats.size === 0) {
      logger.warn(`[S3] Skipping upload of zero-byte file: ${filePath}`);
      return undefined;
    }

    fileStream = createReadStream(filePath);
    const command = new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: fileStream,
      ContentType: contentType,
      ContentLength: fileStats.size,
    });

    const result = await s3Client.send(command);
    logger.info(`[S3] Upload complete for ${key}. ETag: ${result.ETag}`);
    return result.ETag;
  } catch (error: any) {
    logger.error(
      `[S3] Upload failed for ${filePath} to key ${key}:`,
      error.message || error,
    );
    throw new Error(
      `S3 upload failed for ${filePath}: ${error.message || error}`,
    );
  } finally {
    if (fileStream && !fileStream.destroyed) {
      fileStream.destroy();
    }
  }
}

/**
 * Deletes an object from S3.
 * @param bucket The S3 bucket name.
 * @param key The S3 object key to delete.
 */
export async function deleteFromS3(bucket: string, key: string): Promise<void> {
  logger.info(`[S3] Attempting delete: s3://${bucket}/${key}`);
  try {
    const command = new DeleteObjectCommand({ Bucket: bucket, Key: key });
    await s3Client.send(command);
    logger.info(`[S3] Deletion complete for ${key}`);
  } catch (error: any) {
    logger.error(
      `[S3] Deletion failed for key ${key}:`,
      error.message || error,
    );
  }
}
