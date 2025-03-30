import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      reportsDirectory: './coverage',
      include: ['src/**/*.ts'],
      exclude: [
        'src/db/**',
        'src/config/**',
        'src/index.ts',
        'src/app.ts',
        'src/errors/**',
        'src/middleware/**',
        'src/utils/**',
        '**/*.test.ts',
        '**/*.spec.ts',
      ],
      all: true,
    },
    setupFiles: ['./tests/setup.ts'],
    clearMocks: true,
  },
});
