import { Readable } from 'node:stream';
import { createReadStream, createWriteStream } from 'node:fs';
import { unlink, stat, readdir } from 'node:fs/promises';
import {
  GetObjectCommand,
  PutObjectCommand,
  DeleteObjectCommand,
  S3ServiceException,
} from '@aws-sdk/client-s3';
import { lookup } from 'mime-types';
import { join, relative } from 'node:path';

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
  logContext: object = {},
): Promise<void> {
  const s3LogContext = {
    ...logContext,
    operations: 'download',
    bucket,
    key: downloadPath,
  };
  logger.info(s3LogContext, `[S3] Attempting download`);

  try {
    const command = new GetObjectCommand({ Bucket: bucket, Key: key });
    const { Body, ContentLength } = await s3Client.send(command);

    if (!Body || !(Body instanceof Readable)) {
      throw new Error(`Failed to get readable stream for S3 object: ${key}`);
    }

    const sizeMB = ContentLength
      ? (ContentLength / 1024 / 1024).toFixed(2)
      : 'unknown';

    logger.info(
      { ...s3LogContext, sizeMB },
      `[S3] Downloading ${sizeMB} MB...`,
    );

    const writer = createWriteStream(downloadPath);

    await new Promise<void>((resolve, reject) => {
      writer.on('finish', () => {
        logger.debug(s3LogContext, `[S3] Write stream finshed.`);
        resolve();
      });

      writer.on('error', (err) => {
        logger.error({ ...logContext, err }, `[S3] File write stream error`);
        unlink(downloadPath).catch(() => {});
        reject(err);
      });

      Body.on('error', (err) => {
        logger.error({ ...logContext, err }, `[S3] S3 read and stream error.`);
        writer.close();
        unlink(downloadPath).catch(() => {});
        reject(err);
      });

      let bytesDownloaded = 0;
      Body.on('data', (chunk) => {
        bytesDownloaded += chunk.length;
      });

      Body.on('end', () => {
        logger.debug(
          { ...s3LogContext, bytes: bytesDownloaded },
          `[S3] S3 Body stream ended.`,
        );
      });

      Body.pipe(writer);
    });

    logger.info(s3LogContext, `[S3] Download complete`);
  } catch (error: any) {
    let errorMessage = 'Unknown S3 download error';
    if (error instanceof S3ServiceException) {
      errorMessage = `S3 Error (${error.name}, status ${error.$metadata.httpStatusCode}): ${error.message}`;
    } else if (error instanceof Error) {
      errorMessage = error.message;
    }

    logger.error(
      { ...s3LogContext, error: errorMessage },
      `[S3] Download failed.`,
    );

    try {
      await stat(downloadPath);
      await unlink(downloadPath);
      logger.warn(`[S3] Cleaned up partial download file.`);
    } catch (cleanupError: any) {
      if (cleanupError.code !== 'ENOENT') {
        logger.error(
          { ...s3LogContext, cleanupErr: cleanupError },
          `[S3] Error cleaning up partial download.`,
        );
      }
    }

    throw new Error(`[S3] Download failed for ${key}: ${errorMessage}`);
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
  logContext: object = {},
): Promise<string | undefined> {
  const s3LogContext = {
    ...logContext,
    operations: 'upload',
    bucket,
    key,
    filePath,
    contentType,
  };
  logger.info(s3LogContext, `[S3] Attempting single file upload`);

  let fileStream: Readable | undefined;
  try {
    const fileStats = await stat(filePath);
    if (fileStats.size === 0) {
      logger.warn(s3LogContext, `[S3] Uploading zero-byte file.`);
    }

    fileStream = createReadStream(filePath);

    fileStream.on('error', (err) => {
      logger.error(
        { ...s3LogContext, err },
        `[S3] File read stream error during upload.`,
      );

      if (!fileStream?.destroyed) fileStream?.destroy();
    });

    const command = new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: fileStream,
      ContentType: contentType,
      ContentLength: fileStats.size,
    });

    logger.debug(s3LogContext, `[S3] Sending PutObject command...`);
    const result = await s3Client.send(command);
    logger.info(
      { ...s3LogContext, ETag: result.ETag },
      `[S3] Upload complete.`,
    );
    return result.ETag;
  } catch (error: unknown) {
    let errorMessage = 'Unknown S3 upload error';
    if (error instanceof S3ServiceException) {
      errorMessage = `S3 Error (${error.name}, status ${error.$metadata.httpStatusCode}): ${error.message}`;
    } else if (error instanceof Error) {
      errorMessage = error.message;
    }

    logger.error(
      { ...s3LogContext, error: errorMessage },
      `[S3] Upload failed.`,
    );

    throw new Error(
      `S3 upload failed for ${filePath} to ${filePath} to ${key}: ${errorMessage}`,
    );
  } finally {
    if (fileStream && !fileStream.destroyed) {
      fileStream.destroy();
      logger.debug(
        s3LogContext,
        '[S3] File read stream explicitly destroyed after upload apptempt.',
      );
    }
  }
}

/**
 * Uploads all files from a local directory recursively to an S3 prefix
 * @param localDirPath
 * @param bucket
 * @param s3Prefix
 * @param logContext
 */
export async function uploadDirectoryToS3(
  localDirPath: string,
  bucket: string,
  s3Prefix: string,
  logContext: object = {},
): Promise<{ uploaded: number; failed: number }> {
  const dirLogContext = {
    ...logContext,
    operations: 'upload_dir',
    localDirPath,
    bucket,
    s3Prefix,
  };
  logger.info(dirLogContext, '[S3] Starting directory upload');

  let uploadCount = 0;
  let failedCount = 0;
  const uploadPromises: Promise<void>[] = [];

  try {
    const entries = await readdir(localDirPath, { withFileTypes: true });

    for (const entry of entries) {
      let localEntryPath = join(localDirPath, entry.name);

      if (entry.isDirectory()) {
        const subDirS3Prefix = `${s3Prefix}${entry.name}/`;

        logger.debug(
          {
            ...dirLogContext,
            subDir: localEntryPath,
            subPrefix: subDirS3Prefix,
          },
          '[S3] Processing subdirectory',
        );

        uploadPromises.push(
          uploadDirectoryToS3(
            localEntryPath,
            bucket,
            subDirS3Prefix,
            logContext,
          ).then((result) => {
            uploadCount += result.uploaded;
            failedCount += result.failed;
          }),
        );
      } else if (entry.isFile()) {
        const fileKey = `${s3Prefix}${entry.name}`;

        const contentKey = lookup(entry.name) || 'application/octet-stream';

        const fileLogContext = {
          ...dirLogContext,
          file: entry.name,
          key: fileKey,
          contentKey,
        };

        logger.debug(
          fileLogContext,
          '[S3] Queuing file for upload from directory',
        );

        uploadPromises.push(
          uploadToS3(bucket, fileKey, localEntryPath, contentKey, logContext)
            .then(() => {
              uploadCount++;
            })
            .catch((err) => {
              logger.error(
                { ...fileLogContext, err },
                '[S3] Failed to upload file during directory upload',
              );
              failedCount++;
            }),
        );
      } else {
        logger.warn(
          { ...dirLogContext, entryName: entry.name },
          '[S3] Skipping non-file/non-directory entry',
        );
      }
    }

    await Promise.all(uploadPromises);

    logger.info(
      { ...dirLogContext, uploaded: uploadCount, failed: failedCount },
      `[S3] Directory upload finished.`,
    );

    return { uploaded: uploadCount, failed: failedCount };
  } catch (error: any) {
    logger.error(
      { ...dirLogContext, error: error.message },
      '[S3] Error processing directory upload',
    );

    if (error.code === 'ENOENT') {
      logger.error(dirLogContext, '[S3] Local directory not found for upload.');

      return { uploaded: 0, failed: 1 };
    }

    return { uploaded: uploadCount, failed: failedCount + 1 };
  }
}

/**
 * Delete an object from S3
 * @param bucket
 * @param key
 * @param logContext
 */
export async function deleteFromS3(
  bucket: string,
  key: string,
  logContext: object = {},
): Promise<void> {
  const s3LogContext = { ...logContext, operation: 'delete', bucket, key };
  logger.info(s3LogContext, `[S3] Attempting delete`);

  try {
    const command = new DeleteObjectCommand({ Bucket: bucket, Key: key });
    await s3Client.send(command);
    logger.info(s3LogContext, `[S3] Deletion complete.`);
  } catch (error: unknown) {
    let errorMessage = 'Unknown S3 delete error';

    if (error instanceof S3ServiceException && error.name === 'NoSuchKey') {
      logger.warn(s3LogContext, `[S3] Dletion skipped: Key not found.`);
      return;
    } else if (error instanceof S3ServiceException) {
      errorMessage = `S3 Error (${error.name}, status ${error.$metadata.httpStatusCode}): ${error.message}`;
    }

    logger.error(
      { ...s3LogContext, error: errorMessage },
      `[S3] Deletion failed.`,
      // throw new Error(`[S3] Deletion failed for ${key}: ${errorMessage}`);
    );
  }
}
