import { filesystem } from '@rjweb/utils';
import { defineConfig } from 'drizzle-kit';
import path from 'path';

let env: Record<string, string>;
try {
  env = filesystem.env('../.env', { async: false });
} catch {
  env = process.env as Record<string, string>;
}

const url = (env.DATABASE_URL ?? 'sqlite://./data/database.db').replace('sqlite://', '').replace('sqlite:', '');

export default defineConfig({
  dialect: 'sqlite',
  schema: './src/schema.ts',
  out: './migrations',
  breakpoints: false,
  dbCredentials: {
    url: path.join(process.cwd(), '..', url),
  },
});
