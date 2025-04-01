import { drizzle } from 'drizzle-orm/mysql2';
import mysql from 'mysql2/promise';

import { logger } from '../config/logger';
import * as schema from './schema';
import { ENV } from '../config/env';

const poolConnection = mysql.createPool({
  host: ENV.DB_HOST,
  user: ENV.DB_USER,
  password: ENV.DB_PASSWORD,
  database: ENV.DB_NAME,
  port: ENV.DB_PORT,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

logger.info(
  `[DB] Creating connection pool for ${ENV.DB_USER}@${ENV.DB_HOST}:${ENV.DB_PORT}/${ENV.DB_NAME}`,
);

export const db = drizzle(poolConnection, { schema, mode: 'default' });

logger.info('[DB] Drizzle ORM initialized.');

export { schema };

export async function closeDbConnection(): Promise<void> {
  logger.info('[DB] Closing database connection pool...');
  try {
    await poolConnection.end();
    logger.info('[DB] Database connection pool closed.');
  } catch (error: any) {
    logger.error('[DB] Error closing connection pool:', error.message);
  }
}
