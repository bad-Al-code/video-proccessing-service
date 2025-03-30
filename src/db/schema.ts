import { relations, sql } from 'drizzle-orm';
import {
  mysqlTable,
  varchar,
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
  createdAt: timestamp('created_at')
    .default(sql`CURRENT_TIMESTAMP`)
    .notNull(),
  updatedAt: timestamp('updated_at')
    .default(sql`CURRENT_TIMESTAMP`)
    .onUpdateNow()
    .notNull(),
  uploadedAt: timestamp('uploaded_at'),
});

export type VideoStatus = (typeof videoStatuses)[number];
