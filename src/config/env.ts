import 'dotenv/config';
import { z } from 'zod';

const envSchema = z.object({
  PORT: z.string().regex(/^\d+$/).transform(Number),
  MAX_FILE_SIZE_MB: z.string().regex(/^\d+$/).transform(Number),

  DB_HOST: z.string().min(1, 'DB_HOST is required'),
  DB_PORT: z.string().regex(/^\d+$/).transform(Number),
  DB_NAME: z.string().min(1, 'DB_NAME is required'),
  DB_USER: z.string().min(1, 'DB_USER is required'),
  DB_PASSWORD: z.string().min(1, 'DB_PASSWORD is required'),
  // DB_ROOT_PASSWORD: z.string().min(1, 'DB_ROOT_PASSWORD is required'),

  DATABASE_URL: z.string(),

  AWS_S3_BUCKET_NAME: z.string().min(1),
  AWS_REGION: z.string().min(1),
  AWS_ACCESS_KEY_ID: z.string().min(1),
  AWS_SECRET_ACCESS_KEY: z.string().min(1),
  RABBITMQ_USER: z.string().min(1),
  RABBITMQ_PASSWORD: z.string().min(1),
  RABBITMQ_HOST: z.string().min(1).default('rabbitmq'),
  RABBITMQ_NODE_PORT: z.coerce.number().int().positive().default(5672),
  RABBITMQ_VHOST: z.string().startsWith('/').default('/'),
});

const parsedEnv = envSchema.safeParse(process.env);

if (!parsedEnv.success) {
  console.error('Invalid Environment Variables:', parsedEnv.error.format());

  throw new Error(
    `Invalid Environment Variables: ${JSON.stringify(parsedEnv.error.format())}`,
  );
}

export const ENV = parsedEnv.data;
