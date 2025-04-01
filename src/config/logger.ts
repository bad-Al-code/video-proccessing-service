import pino from 'pino';

const isProduction = process.env.NODE_ENV === 'production ';

const loggerOptions: pino.LoggerOptions = {
  level: isProduction ? 'info' : 'debug',
  formatters: {
    level: (label) => ({ level: label }),
  },
  base: {
    service: 'video-processing-service',
    pid: process.pid,
  },
  timestamp: pino.stdTimeFunctions.isoTime,
};

const transport = isProduction
  ? undefined
  : pino.transport({
      target: 'pino-pretty',
      options: {
        colorize: true,
        translateTime: 'SYS:yyyy-mm-dd HH:MM:ss.l',
        ignore: 'pid,hostname,service',
      },
    });

export const logger = pino(loggerOptions, transport);

logger.info('Structured logger initialized.');

process.on('SIGINT', () => {
  logger.flush();
  process.exit(0);
});
process.on('SIGTERM', () => {
  logger.flush();
  process.exit(0);
});
