import { Readable, Writable } from 'node:stream';
import { createReadStream, createWriteStream, fstat } from 'node:fs';
import { unlink } from 'node:fs/promises';
import {
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
} from '@aws-sdk/client-s3';

import { s3Client } from '../config/s3Client';

export async function downloadFromS3(
  Bucket: string,
  key: string,
  downloadPath: string,
): Promise<void> {
  try {
    const command = new GetObjectCommand({ Bucket: Bucket, Key: key });
    const { Body } = await s3Client.send(command);

    if (!Body || !(Body instanceof Readable)) {
      throw new Error(`Failed to get readable stream from S3 object key`);
    }

    const writer = createWriteStream(downloadPath);

    await new Promise((resolve, reject) => {
      Body.pipe(writer as Writable)
        .on('finish', resolve)
        .on('error', reject);
    });

    console.log(`[S3] Download complte: ${downloadPath}`);
  } catch (error: any) {
    console.error(`[S3] Download failed for s3://${Bucket}/${key}: `, error);

    try {
      await unlink(downloadPath);
    } catch (cleanupError) {}

    throw new Error(`[S3] Download failed: ${error.message || error}`);
  }
}

export async function uploadToS3(
  bucket: string,
  key: string,
  filePath: string,
  contentType: string,
): Promise<string | undefined> {
  console.log([`[S3] Attempting upload: ${filePath} to s3://${bucket}/${key}`]);

  let fileStream;

  try {
    fileStream = createReadStream(filePath);
    const command = new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: fileStream,
      ContentType: contentType,
    });

    const result = await s3Client.send(command);
    console.log(`[S3] Upload complete for ${key}. ETag: ${result.ETag}`);

    return result.ETag;
  } catch (error: any) {
    console.error(`[S3] Upload failed for ${filePath} to key ${key}: `, error);

    throw new Error(`S3 upload failed: ${error.message || error}`);
  } finally {
    if (fileStream && !fileStream.destroyed) {
      fileStream.destroy();
    }
  }
}

export async function deleteFromS3(bucket: string, key: string): Promise<void> {
  console.log(`[S3] Atempting delete: s3://${bucket}/${key}`);

  try {
    const command = new DeleteObjectCommand({ Bucket: bucket, Key: key });
    await s3Client.send(command);
    console.log(`[S3] Deletion complete for ${key}`);
  } catch (error: any) {
    console.error(`[S3] Deletion failed for key: ${key}: `, error);
  }
}
