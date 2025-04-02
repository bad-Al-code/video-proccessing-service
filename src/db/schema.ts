import { relations, sql } from 'drizzle-orm';
import {
  mysqlTable,
  varchar,
  json,
  bigint,
  mysqlEnum,
  text,
  timestamp,
} from 'drizzle-orm/mysql-core';

export const videoStatuses = [
  'PENDING_UPLOAD',
  'UPLOAD_FAILED',
  'PROCESSING',
  'READY',
  'ERROR',
  'UPLOADED',
] as const;

export type VideoStatus = (typeof videoStatuses)[number];

export type VideoMetadata = {
  durationSeconds?: number | null;
  width?: number | null;
  height?: number | null;
  formatName?: string | null;
  bitRate?: number | null;
} | null;

export type ProcessedFiles = {
  thumbnail?: string | null;
  [resolution: number]: string;
} | null;

export const videos = mysqlTable('videos', {
  id: varchar('id', { length: 36 }).primaryKey(),
  originalFilename: varchar('original_filename', { length: 255 }).notNull(),
  objectStorageKey: varchar('object_storage_key', { length: 1024 }),
  mimeType: varchar('mime_type', { length: 100 }).notNull(),
  sizeBytes: bigint('size_bytes', { mode: 'number', unsigned: true }).notNull(),
  status: mysqlEnum('status', videoStatuses)
    .default('PENDING_UPLOAD')
    .notNull(),
  title: varchar('title', { length: 255 }),
  description: text('description'),
  durationSeconds: bigint('duration_seconds', {
    mode: 'number',
    unsigned: true,
  }),
  metadata: json('metadata').$type<VideoMetadata>().default(null),
  processedFiles: json('processed_files').$type<ProcessedFiles>().default(null),

  createdAt: timestamp('created_at')
    .default(sql`CURRENT_TIMESTAMP`)
    .notNull(),
  updatedAt: timestamp('updated_at')
    .default(sql`CURRENT_TIMESTAMP`)
    .onUpdateNow()
    .notNull(),
  uploadedAt: timestamp('uploaded_at'),
  processedAt: timestamp('processed_at'),
});
