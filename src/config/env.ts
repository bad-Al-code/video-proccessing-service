import 'dotenv/config';
import { z } from 'zod';
import { logger } from './logger';

const CommaSeparatedStringToNumberArray = z
  .string()
  .min(1, 'PROCESSING_RESOLUTIONS cannot be empty.')
  .transform((val, ctx) => {
    const resolutions = val
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
      .map(Number);

    if (resolutions.some(isNaN)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message:
          'PROCESSING_RESOLUTIONS must be comma-separated numbers (e.g., "1080,720,480"). Found non-numeric value.',
      });
      return z.NEVER;
    }

    const validResolutions = [1080, 720, 480, 360, 240, 144];

    if (resolutions.some((r) => !validResolutions.includes(r))) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `Invalid resolution found. Allowed resolutions are: ${validResolutions.join(', ')}.`,
      });
      return z.NEVER;
    }

    if (resolutions.some((r) => r <= 0)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'Resolutions must be positive numbers.',
      });
      return z.NEVER;
    }

    return [...new Set(resolutions)].sort((a, b) => b - a);
  });

const envSchema = z
  .object({
    NODE_ENV: z
      .enum(['development', 'production', 'test'])
      .default('development'),
    DB_HOST: z.string().min(1),
    DB_PORT: z.coerce.number().int().positive(),
    DB_NAME: z.string().min(1),
    DB_USER: z.string().min(1),
    DB_PASSWORD: z.string().min(1),
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

    PROCESSING_RESOLUTIONS: CommaSeparatedStringToNumberArray,
    THUMBNAIL_SIZE: z
      .string()
      .default('640x?')
      .describe('FFmpeg size string for thumbnail (e.g., 640x?, 320x180)'),
    THUMBNAIL_TIMEMARK: z
      .string()
      .default('10%')
      .describe('FFmpeg time mark for thumbnail (e.g., 10%, 00:00:05)'),
  })
  .refine(
    (data) =>
      data.DATABASE_URL ??
      `mysql://${data.DB_USER}:${data.DB_PASSWORD}@${data.DB_HOST}:${data.DB_PORT}/${data.DB_NAME}`,
    {
      message: 'DATABASE_URL is required if not automatically constructed',
      path: ['DATABASE_URL'],
    },
  );

const parsedEnv = envSchema.safeParse(process.env);

if (!parsedEnv.success) {
  logger.error(
    'Invalid environment variables:',
    JSON.stringify(parsedEnv.error.format(), null, 4),
  );
  process.exit(1);
}

const envData = {
  ...parsedEnv.data,
  DATABASE_URL:
    parsedEnv.data.DATABASE_URL ??
    `mysql://${parsedEnv.data.DB_USER}:${parsedEnv.data.DB_PASSWORD}@${parsedEnv.data.DB_HOST}:${parsedEnv.data.DB_PORT}/${parsedEnv.data.DB_NAME}`,
};

export const ENV = parsedEnv.data;
logger.info('Environment variables loaded and validated.');
