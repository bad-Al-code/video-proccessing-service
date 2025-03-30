import { drizzle } from 'drizzle-orm/mysql2';
import mysql from 'mysql2/promise';

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

console.log('Database connection pool created.');

export const db = drizzle(poolConnection, { schema, mode: 'default' });

console.log('Drizzle ORM initialized.');

export { schema };
