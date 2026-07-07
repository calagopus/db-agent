import logger from '@/globals/logger';

logger()
  .text('DB Agent Database', (c) => c.yellowBright)
  .text(`(${process.env.NODE_ENV === 'development' ? 'development' : 'production'})`, (c) => c.gray)
  .info();
logger()
  .text('This is not meant to be ran directly, this only provides the database schema', (c) => c.red)
  .info();
