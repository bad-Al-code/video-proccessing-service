import { S3Client } from '@aws-sdk/client-s3';
import { ENV } from './env';

console.log(`[S3] Initializing client for region: ${ENV.AWS_REGION}`);

export const s3Client = new S3Client({
  region: ENV.AWS_REGION,
  credentials: {
    accessKeyId: ENV.AWS_ACCESS_KEY_ID,
    secretAccessKey: ENV.AWS_SECRET_ACCESS_KEY,
  },
});

console.log(`[S3] Client initialized.`);
