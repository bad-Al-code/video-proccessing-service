import 'dotenv/config';

import { vi, beforeEach, afterAll } from 'vitest';

beforeEach(() => {
  vi.clearAllMocks();
});

afterAll(() => {
  console.log(`Finished running tests`);
});
