import { join } from 'node:path';
import { ENV } from './env';

const BASE_DIR = join(__dirname, '..', '..');

export const UPLOAD_DIR = join(BASE_DIR, 'uploads');
export const TEMP_DIR = join(BASE_DIR, 'temp');

export const MAX_FILE_SIZE_MB = ENV.MAX_FILE_SIZE_MB;

export const ALLOWED_EXTENSIONS = [
  '.mp4',
  '.mpeg',
  '.mov',
  '.webm',
  '.avi',
  '.mkv',
];
export const ALLOWED_VIDEO_TYPES = [
  'video/mp4',
  'video/mpeg',
  'video/quicktime',
  'video/webm',
  'video/x-msvideo',
  'video/x-matroska',
];

export const VIDEO_EVENTS_EXCHANGE = 'video_events_topic';
export const VIDEO_UPLOAD_COMPLETED_ROUTING_KEY = 'video.upload.completed';
