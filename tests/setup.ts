import { vi, beforeEach, afterAll } from 'vitest';

process.env.DB_HOST = '127.0.0.1';
process.env.DB_PORT = '3306';
console.log(
  `[Test Setup] Overriding DB_HOST=${process.env.DB_HOST}, DB_PORT=${process.env.DB_PORT}`,
);

import 'dotenv/config';

beforeEach(() => {
  vi.clearAllMocks();
});

afterAll(() => {
  console.log(`Finished running tests`);
});
