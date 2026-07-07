import { integer, blob, index, text, sqliteTable, uniqueIndex } from "drizzle-orm/sqlite-core"

export const databases = sqliteTable(
  'databases',
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
  },
  (cols) => [
		index('databases_database_type_idx').on(cols.database_type),
    uniqueIndex('databases_uuid_short_idx').on(cols.uuid_short),
  ],
);

export const databaseUsers = sqliteTable(
	'database_users',
	{
		uuid: blob().primaryKey().notNull(),
		uuid_short: integer().notNull(),
		database_uuid: blob().notNull().references(() => databases.uuid, { onDelete: 'cascade' }),
		username: text().notNull(),
		password: text().notNull(),
	},
	(cols) => [
		index('database_users_database_uuid_idx').on(cols.database_uuid),
		index('database_users_username_idx').on(cols.username),
		uniqueIndex('database_users_uuid_short_idx').on(cols.uuid_short),
	],
);
