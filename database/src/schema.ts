import { sql } from "drizzle-orm"
import { integer, blob, index, text, sqliteTable, uniqueIndex } from "drizzle-orm/sqlite-core"

export const instances = sqliteTable(
  'instances',
  {
    uuid: blob().primaryKey().notNull(),
    uuid_short: integer().notNull(),
    database_type: text().notNull(),
    suspended: integer({ mode: 'boolean' }).default(false).notNull(),
    memory: integer().notNull(),
    swap: integer().notNull(),
    disk: integer().notNull(),
    io_weight: integer(),
    cpu: integer().notNull(),
    image: text().notNull(),
    image_uid: integer().notNull(),
    image_gid: integer().notNull(),
    volumes: text().notNull(),
    socket_path: text().notNull(),
    timezone: text(),
    env: text().default('{}').notNull(),
    cmd: text(),
    root_password: text(),
    created: integer({ mode: 'timestamp' }).notNull().default(sql`(unixepoch())`),
  },
  (cols) => [
    index('instances_database_type_idx').on(cols.database_type),
    uniqueIndex('instances_uuid_short_idx').on(cols.uuid_short),
  ],
);

export const databases = sqliteTable(
  'databases',
  {
    uuid: blob().primaryKey().notNull(),
    instance_uuid: blob().notNull().references(() => instances.uuid, { onDelete: 'cascade' }),
    name: text().notNull(),
    created: integer({ mode: 'timestamp' }).notNull().default(sql`(unixepoch())`),
  },
  (cols) => [
    index('databases_instance_uuid_idx').on(cols.instance_uuid),
    uniqueIndex('databases_instance_uuid_name_idx').on(cols.instance_uuid, cols.name),
  ],
);

export const users = sqliteTable(
  'users',
  {
    uuid: blob().primaryKey().notNull(),
    uuid_short: integer().notNull(),
    instance_uuid: blob().notNull().references(() => instances.uuid, { onDelete: 'cascade' }),
    database_uuid: blob().references(() => databases.uuid, { onDelete: 'cascade' }),
    username: text().notNull(),
    password: text().notNull(),
    created: integer({ mode: 'timestamp' }).notNull().default(sql`(unixepoch())`),
  },
  (cols) => [
    index('users_instance_uuid_idx').on(cols.instance_uuid),
    index('users_database_uuid_idx').on(cols.database_uuid),
    index('users_username_idx').on(cols.username),
    uniqueIndex('users_uuid_short_idx').on(cols.uuid_short),
  ],
);
