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
        'src/consumers/**',
        '**/*.test.ts',
        '**/*.spec.ts',
        'node_modules/**',
      ],
      all: true,
    },
    setupFiles: ['./tests/setup.ts'],
    include: ['tests/**/*.test.ts'],
    clearMocks: true,
  },
});
