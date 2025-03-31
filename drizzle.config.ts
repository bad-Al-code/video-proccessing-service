import 'dotenv/config';
import { defineConfig } from 'drizzle-kit';

if (
  !process.env.DB_USER ||
  !process.env.DB_PASSWORD ||
  !process.env.DB_HOST ||
  !process.env.DB_PORT ||
  !process.env.DB_NAME
) {
  throw new Error(
    'Missing required database environment variables (DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)',
  );
}

const databaseUrl = `mysql://${process.env.DB_USER}:${process.env.DB_PASSWORD}@${process.env.DB_HOST}:${process.env.DB_PORT}/${process.env.DB_NAME}`;

console.log('Using Database URL for Drizzle Kit:', databaseUrl);

export default defineConfig({
  out: './drizzle',
  schema: './src/db/schema.ts',
  dialect: 'mysql',
  dbCredentials: {
    url: databaseUrl,
  },
  verbose: true,
  strict: true,
});
