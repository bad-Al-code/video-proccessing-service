import 'dotenv/config';
import { z } from 'zod';

const envSchema = z.object({
  NODE_ENV: z
    .enum(['development', 'production', 'test'])
    .default('development'),
  DB_HOST: z.string().min(1),
  DB_PORT: z.coerce.number().int().positive(),
  DB_NAME: z.string().min(1),
  DB_USER: z.string().min(1),
  DB_PASSWORD: z.string().min(1),
  DATABASE_URL: z.string().url().optional(),

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
  console.error(
    'Invalid environment variables:',
    JSON.stringify(parsedEnv.error.format(), null, 4),
  );
  process.exit(1);
}

export const ENV = parsedEnv.data;
console.log('Environment variables loaded and validated.');
