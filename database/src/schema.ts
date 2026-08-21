import { sql } from "drizzle-orm"
import { integer, blob, check, index, text, primaryKey, sqliteTable, uniqueIndex } from "drizzle-orm/sqlite-core"

export const instances = sqliteTable(
  'instances',
  {
    uuid: blob().primaryKey().notNull(),
    uuid_short: integer().notNull(),
    database_type: text().notNull(),
    state: text({ enum: ['offline', 'starting', 'stopping', 'running'] }).default('offline').notNull(),
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
    check('instances_state_check', sql`${cols.state} in ('offline', 'starting', 'stopping', 'running')`),
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
    username: text().notNull(),
    password: text().notNull(),
    created: integer({ mode: 'timestamp' }).notNull().default(sql`(unixepoch())`),
  },
  (cols) => [
    index('users_instance_uuid_idx').on(cols.instance_uuid),
    index('users_username_idx').on(cols.username),
    uniqueIndex('users_uuid_short_idx').on(cols.uuid_short),
  ],
);

export const userDatabases = sqliteTable(
  'user_databases',
  {
    user_uuid: blob().notNull().references(() => users.uuid, { onDelete: 'cascade' }),
    database_uuid: blob().notNull().references(() => databases.uuid, { onDelete: 'cascade' }),
    permission: text({ enum: ['read_only', 'read_write'] }).notNull(),
    created: integer({ mode: 'timestamp' }).notNull().default(sql`(unixepoch())`),
  },
  (cols) => [
    primaryKey({ columns: [cols.user_uuid, cols.database_uuid] }),
    index('user_databases_database_uuid_idx').on(cols.database_uuid),
    check('user_databases_permission_check', sql`${cols.permission} in ('read_only', 'read_write')`),
  ],
);
