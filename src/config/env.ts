import 'dotenv/config';
import { z } from 'zod';

import { logger } from './logger';

const processingResolutionSchema = z.enum(['1080p', '720p', '480p', '360p']);
export type ProcessingResolution = z.infer<typeof processingResolutionSchema>;

const envSchema = z.object({
  NODE_ENV: z
    .enum(['development', 'production', 'test'])
    .default('development'),

  DB_HOST: z.string().min(1),
  DB_PORT: z.coerce.number().int().positive(),
  DB_NAME: z.string().min(1),
  DB_USER: z.string().min(1),
  DB_PASSWORD: z.string().min(1),
  DATABASE_URL: z.string().min(1),

  AWS_S3_BUCKET_NAME: z.string().min(1),
  AWS_REGION: z.string().min(1),
  AWS_ACCESS_KEY_ID: z.string().min(1),
  AWS_SECRET_ACCESS_KEY: z.string().min(1),

  RABBITMQ_USER: z.string().min(1),
  RABBITMQ_PASSWORD: z.string().min(1),
  RABBITMQ_HOST: z.string().min(1).default('rabbitmq'),
  RABBITMQ_NODE_PORT: z.coerce.number().int().positive().default(5672),
  RABBITMQ_VHOST: z.string().startsWith('/').default('/'),

  PROCESSING_RESOLUTIONS: z
    .string()
    .min(1)
    .default('720p,480p')
    .transform((val) =>
      val
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean),
    ),

  THUMBNAIL_TIMESTAMPS: z
    .string()
    .min(1)
    .default('10%')
    .transform((val) =>
      val
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean),
    )
    .pipe(
      z
        .array(z.string().min(1))
        .min(1)
        .describe(
          'Comma-separated timestamps for thumbnails (e.g., 10%,50% or 00:00:05,00:00:15)',
        ),
    ),

  THUMBNAIL_SIZE: z
    .string()
    .min(3)
    .default('640x?')
    .describe("Size for generated thumbnails (e.g., '640x?' or '320x180')"),

  FFMPEG_CONCURRENCY: z.coerce
    .number()
    .int()
    .positive()
    .default(2)
    .describe('Maximum number of concurrent FFmpeg processes'),

  WATERMARK_S3_KEY: z
    .string()
    .optional()
    .describe('S3 key for the watermark image (leave empty to disable)'),

  WATERMARK_POSITION: z
    .enum(['top_left', 'top_right', 'bottom_left', 'bottom_right', 'center'])
    .default('bottom_right')
    .describe('Position for the watermark overlay'),

  HLS_RESOLUTION: processingResolutionSchema
    .default('480p')
    .describe(
      'Resolution (from PROCESSING_RESOLUTIONS) to use for HLS generation',
    ),
});

const parsedEnv = envSchema.safeParse(process.env);

if (!parsedEnv.success) {
  logger.error(
    { errors: parsedEnv.error.format() },
    'Invalid environment variables',
  );
  process.exit(1);
}

export const ENV = parsedEnv.data;
logger.info('Environment variables loaded and validated.');
