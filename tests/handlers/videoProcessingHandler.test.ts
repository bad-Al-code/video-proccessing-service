import { describe, it, expect, vi, beforeEach, Mock } from 'vitest';
import { ConsumeMessage } from 'amqplib';

vi.mock('node:fs/promises', () => {
  const mockMkdir = vi.fn().mockResolvedValue(undefined);
  const mockRm = vi.fn().mockResolvedValue(undefined);
  return {
    __esModule: true,
    default: {
      mkdir: mockMkdir,
      rm: mockRm,
    },
    mkdir: mockMkdir,
    rm: mockRm,
    _mockMkdir: mockMkdir,
    _mockRm: mockRm,
  };
});

const fsPromisesMocks = await vi.importMock<
  typeof import('node:fs/promises') & { _mockMkdir: Mock; _mockRm: Mock }
>('node:fs/promises');
const mockMkdir = fsPromisesMocks._mockMkdir;
const mockRm = fsPromisesMocks._mockRm;

const mockDbUpdateSetWhere = vi.fn().mockResolvedValue({ rowCount: 1 });
const mockDbUpdateSet = vi.fn(() => ({ where: mockDbUpdateSetWhere }));
const mockDbUpdate = vi.fn(() => ({ set: mockDbUpdateSet }));
const mockDb = { update: mockDbUpdate };
const mockSchema = { videos: { id: 'videos.id' } };
const mockEq = vi.fn((field, value) => `mockEq(${field}, ${value})`);
vi.mock('../db', () => ({
  db: mockDb,
  schema: mockSchema,
  eq: mockEq,
}));

const MOCK_BUCKET = 'mock-video-bucket';
const mockEnv = {
  AWS_S3_BUCKET_NAME: MOCK_BUCKET,
};
vi.mock('../config/env', () => ({
  ENV: mockEnv,
}));

vi.mock('../utils/s3Utils', () => {
  const mockDownloadFromS3 = vi.fn().mockResolvedValue(undefined);
  const mockUploadToS3 = vi.fn().mockResolvedValue('mock-etag-123');
  return {
    __esModule: true,
    default: {
      downloadFromS3: mockDownloadFromS3,
      uploadToS3: mockUploadToS3,
    },
    downloadFromS3: mockDownloadFromS3,
    uploadToS3: mockUploadToS3,
    _mockDownloadFromS3: mockDownloadFromS3,
    _mockUploadToS3: mockUploadToS3,
  };
});
const s3UtilsMocks = await vi.importMock<
  typeof import('../../src/utils/s3.util') & {
    _mockDownloadFromS3: Mock;
    _mockUploadToS3: Mock;
  }
>('../utils/s3Utils');
const mockDownloadFromS3 = s3UtilsMocks._mockDownloadFromS3;
const mockUploadToS3 = s3UtilsMocks._mockUploadToS3;

const mockTranscodeResult720 = {
  outputPath:
    '/tmp/video-processing-service/test-video-id/test-video-id_720p.mp4',
  resolution: '720p',
  s3Key: 'processed/test-video-id/test-video-id_720p.mp4',
};
const mockTranscodeResult480 = {
  outputPath:
    '/tmp/video-processing-service/test-video-id/test-video-id_480p.mp4',
  resolution: '480p',
  s3Key: 'processed/test-video-id/test-video-id_480p.mp4',
};
const mockThumbnailResult = {
  outputPath:
    '/tmp/video-processing-service/test-video-id/test-video-id_thumbnail.jpg',
  s3Key: 'thumbnails/test-video-id/test-video-id_thumbnail.jpg',
};
vi.mock('../utils/ffmpegUtils', () => {
  const mockTranscodeToResolution = vi
    .fn()
    .mockResolvedValueOnce(mockTranscodeResult720)
    .mockResolvedValueOnce(mockTranscodeResult480);
  const mockGenerateThumbnail = vi.fn().mockResolvedValue(mockThumbnailResult);
  return {
    __esModule: true,
    default: {
      transcodeToResolution: mockTranscodeToResolution,
      generateThumbnail: mockGenerateThumbnail,
    },
    transcodeToResolution: mockTranscodeToResolution,
    generateThumbnail: mockGenerateThumbnail,
    _mockTranscodeToResolution: mockTranscodeToResolution,
    _mockGenerateThumbnail: mockGenerateThumbnail,
  };
});
const ffmpegUtilsMocks = await vi.importMock<
  typeof import('../../src/utils/ffmpeg.util') & {
    _mockTranscodeToResolution: Mock;
    _mockGenerateThumbnail: Mock;
  }
>('../utils/ffmpegUtils');
const mockTranscodeToResolution = ffmpegUtilsMocks._mockTranscodeToResolution;
const mockGenerateThumbnail = ffmpegUtilsMocks._mockGenerateThumbnail;

const mockPublishVideoEvent = vi.fn().mockResolvedValue(true);
vi.mock('../producers/VideoEventProducer', () => ({
  videoEventProducer: {
    publishVideoEvent: mockPublishVideoEvent,
  },
}));

import { handleVideoUploadEvent } from '../../src/handlers/VideoProcessingHandler';
import {
  VIDEO_PROCESSING_COMPLETED_ROUTING_KEY,
  VIDEO_PROCESSING_FAILED_ROUTING_KEY,
} from '../../src/config/constants';

describe('handleVideoUploadEvent', () => {
  const mockPayload = {
    videoId: 'test-video-id',
    s3Key: 'videos/test-video-id.mp4',
    originalFilename: 'test-original.mp4',
    mimeType: 'video/mp4',
  };
  const mockConsumeMsg = {} as ConsumeMessage;

  beforeEach(() => {
    mockMkdir.mockClear().mockResolvedValue(undefined);
    mockRm.mockClear().mockResolvedValue(undefined);
    mockDownloadFromS3.mockClear().mockResolvedValue(undefined);
    mockUploadToS3.mockClear().mockResolvedValue('mock-etag-123');
    mockTranscodeToResolution
      .mockClear()
      .mockResolvedValueOnce(mockTranscodeResult720)
      .mockResolvedValueOnce(mockTranscodeResult480);
    mockGenerateThumbnail.mockClear().mockResolvedValue(mockThumbnailResult);
    mockDbUpdateSetWhere.mockClear().mockResolvedValue({ rowCount: 1 });
    mockDbUpdate.mockClear();
    mockDbUpdateSet.mockClear();
    mockPublishVideoEvent.mockClear().mockResolvedValue(true);
  });

  it('should process video successfully, update DB, publish event, and return true', async () => {
    const result = await handleVideoUploadEvent(mockPayload, mockConsumeMsg);

    expect(result).toBe(true);

    expect(mockDbUpdate).toHaveBeenCalledTimes(2);
    expect(mockDbUpdateSet).toHaveBeenNthCalledWith(1, {
      status: 'PROCESSING',
    });
    expect(mockDbUpdateSetWhere).toHaveBeenNthCalledWith(
      1,
      mockEq(mockSchema.videos.id, mockPayload.videoId),
    );
    expect(mockDbUpdateSet).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        status: 'READY',
        processedAt: expect.any(Date),
        s3Key720p: mockTranscodeResult720.s3Key,
        s3Key480p: mockTranscodeResult480.s3Key,
        s3KeyThumbnail: mockThumbnailResult.s3Key,
      }),
    );
    expect(mockDbUpdateSetWhere).toHaveBeenNthCalledWith(
      2,
      mockEq(mockSchema.videos.id, mockPayload.videoId),
    );

    expect(mockDownloadFromS3).toHaveBeenCalledOnce();
    expect(mockDownloadFromS3).toHaveBeenCalledWith(
      MOCK_BUCKET,
      mockPayload.s3Key,
      expect.stringContaining(`original_${mockPayload.originalFilename}`),
    );
    expect(mockUploadToS3).toHaveBeenCalledTimes(3);
    expect(mockUploadToS3).toHaveBeenCalledWith(
      MOCK_BUCKET,
      mockTranscodeResult720.s3Key,
      mockTranscodeResult720.outputPath,
      'video/mp4',
    );
    expect(mockUploadToS3).toHaveBeenCalledWith(
      MOCK_BUCKET,
      mockTranscodeResult480.s3Key,
      mockTranscodeResult480.outputPath,
      'video/mp4',
    );
    expect(mockUploadToS3).toHaveBeenCalledWith(
      MOCK_BUCKET,
      mockThumbnailResult.s3Key,
      mockThumbnailResult.outputPath,
      'image/jpeg',
    );

    expect(mockTranscodeToResolution).toHaveBeenCalledTimes(2);
    expect(mockGenerateThumbnail).toHaveBeenCalledOnce();

    expect(mockPublishVideoEvent).toHaveBeenCalledOnce();
    expect(mockPublishVideoEvent).toHaveBeenCalledWith(
      VIDEO_PROCESSING_COMPLETED_ROUTING_KEY,
      expect.objectContaining({
        videoId: mockPayload.videoId,
        status: 'READY',
        keys: {
          s3Key720p: mockTranscodeResult720.s3Key,
          s3Key480p: mockTranscodeResult480.s3Key,
          s3KeyThumbnail: mockThumbnailResult.s3Key,
        },
      }),
    );

    expect(mockMkdir).toHaveBeenCalledTimes(2);
    expect(mockRm).toHaveBeenCalledOnce();
    expect(mockRm).toHaveBeenCalledWith(
      expect.stringContaining(mockPayload.videoId),
      { recursive: true, force: true },
    );
  });

  it('should return false, update DB to ERROR, and publish fail event if S3 download fails', async () => {
    const downloadError = new Error('S3 Download Access Denied');
    mockDownloadFromS3.mockRejectedValueOnce(downloadError);

    const result = await handleVideoUploadEvent(mockPayload, mockConsumeMsg);

    expect(result).toBe(false);

    expect(mockDbUpdate).toHaveBeenCalledTimes(2);
    expect(mockDbUpdateSet).toHaveBeenNthCalledWith(1, {
      status: 'PROCESSING',
    });
    expect(mockDbUpdateSet).toHaveBeenNthCalledWith(2, { status: 'ERROR' });

    expect(mockPublishVideoEvent).toHaveBeenCalledOnce();
    expect(mockPublishVideoEvent).toHaveBeenCalledWith(
      VIDEO_PROCESSING_FAILED_ROUTING_KEY,
      expect.objectContaining({
        videoId: mockPayload.videoId,
        status: 'ERROR',
        error: downloadError.message,
        originalS3Key: mockPayload.s3Key,
      }),
    );

    expect(mockTranscodeToResolution).not.toHaveBeenCalled();
    expect(mockGenerateThumbnail).not.toHaveBeenCalled();
    expect(mockUploadToS3).not.toHaveBeenCalled();

    expect(mockRm).toHaveBeenCalledOnce();
  });

  it('should return false, update DB to ERROR, and publish fail event if transcoding fails', async () => {
    const transcodeError = new Error('FFmpeg Invalid Input');
    (mockTranscodeToResolution as Mock)
      .mockReset()
      .mockRejectedValueOnce(transcodeError);

    const result = await handleVideoUploadEvent(mockPayload, mockConsumeMsg);

    expect(result).toBe(false);

    expect(mockDbUpdate).toHaveBeenCalledTimes(2);
    expect(mockDbUpdateSet).toHaveBeenNthCalledWith(1, {
      status: 'PROCESSING',
    });
    expect(mockDbUpdateSet).toHaveBeenNthCalledWith(2, { status: 'ERROR' });

    expect(mockPublishVideoEvent).toHaveBeenCalledOnce();
    expect(mockPublishVideoEvent).toHaveBeenCalledWith(
      VIDEO_PROCESSING_FAILED_ROUTING_KEY,
      expect.objectContaining({
        videoId: mockPayload.videoId,
        status: 'ERROR',
        error: transcodeError.message,
      }),
    );

    expect(mockUploadToS3).not.toHaveBeenCalled();

    expect(mockRm).toHaveBeenCalledOnce();
  });

  it('should return false, update DB to ERROR, and publish fail event if S3 upload fails', async () => {
    const uploadError = new Error('S3 Upload Timeout');
    mockUploadToS3
      .mockResolvedValueOnce('etag-1')
      .mockRejectedValueOnce(uploadError)
      .mockResolvedValueOnce('etag-3');

    const result = await handleVideoUploadEvent(mockPayload, mockConsumeMsg);

    expect(result).toBe(false);

    expect(mockDbUpdate).toHaveBeenCalledTimes(2);
    expect(mockDbUpdateSet).toHaveBeenNthCalledWith(1, {
      status: 'PROCESSING',
    });
    expect(mockDbUpdateSet).toHaveBeenNthCalledWith(2, { status: 'ERROR' });

    expect(mockPublishVideoEvent).toHaveBeenCalledOnce();
    expect(mockPublishVideoEvent).toHaveBeenCalledWith(
      VIDEO_PROCESSING_FAILED_ROUTING_KEY,
      expect.objectContaining({
        videoId: mockPayload.videoId,
        status: 'ERROR',
        error: uploadError.message,
      }),
    );

    expect(mockRm).toHaveBeenCalledOnce();
  });

  it('should return true but log error if final DB update to READY fails', async () => {
    const dbUpdateError = new Error('DB connection lost');
    mockDbUpdateSetWhere.mockResolvedValueOnce({ rowCount: 1 });
    mockDbUpdateSetWhere.mockRejectedValueOnce(dbUpdateError);

    const consoleErrorSpy = vi.spyOn(console, 'error');

    const result = await handleVideoUploadEvent(mockPayload, mockConsumeMsg);

    expect(result).toBe(true);

    expect(mockDbUpdate).toHaveBeenCalledTimes(2);
    expect(mockDbUpdateSet).toHaveBeenNthCalledWith(1, {
      status: 'PROCESSING',
    });
    expect(mockDbUpdateSet).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ status: 'READY' }),
    );

    expect(consoleErrorSpy).toHaveBeenCalledWith(
      expect.stringContaining('FAILED to update final DB status to READY'),
      dbUpdateError.message,
    );

    expect(mockPublishVideoEvent).not.toHaveBeenCalled();

    expect(mockRm).toHaveBeenCalledOnce();

    consoleErrorSpy.mockRestore();
  });

  it('should return true and log warning if SUCCESS event publishing fails', async () => {
    mockPublishVideoEvent.mockResolvedValueOnce(false);

    const consoleWarnSpy = vi.spyOn(console, 'warn');

    const result = await handleVideoUploadEvent(mockPayload, mockConsumeMsg);

    expect(result).toBe(true);

    expect(mockPublishVideoEvent).toHaveBeenCalledOnce();
    expect(mockPublishVideoEvent).toHaveBeenCalledWith(
      VIDEO_PROCESSING_COMPLETED_ROUTING_KEY,
      expect.anything(),
    );

    expect(mockDbUpdateSet).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ status: 'READY' }),
    );

    expect(mockRm).toHaveBeenCalledOnce();

    consoleWarnSpy.mockRestore();
  });
});
