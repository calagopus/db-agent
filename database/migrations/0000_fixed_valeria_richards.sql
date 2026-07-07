CREATE TABLE `database_users` (
	`uuid` blob PRIMARY KEY NOT NULL,
	`uuid_short` integer NOT NULL,
	`database_uuid` blob NOT NULL,
	`username` text NOT NULL,
	`password` text NOT NULL,
	FOREIGN KEY (`database_uuid`) REFERENCES `databases`(`uuid`) ON UPDATE no action ON DELETE cascade
);

CREATE INDEX `database_users_database_uuid_idx` ON `database_users` (`database_uuid`);
CREATE INDEX `database_users_username_idx` ON `database_users` (`username`);
CREATE UNIQUE INDEX `database_users_uuid_short_idx` ON `database_users` (`uuid_short`);
CREATE TABLE `databases` (
	`uuid` blob PRIMARY KEY NOT NULL,
	`uuid_short` integer NOT NULL,
	`database_type` text NOT NULL,
	`suspended` integer DEFAULT false NOT NULL,
	`memory` integer NOT NULL,
	`swap` integer NOT NULL,
	`disk` integer NOT NULL,
	`io_weight` integer,
	`cpu` integer NOT NULL,
	`image` text NOT NULL,
	`image_uid` integer NOT NULL,
	`image_gid` integer NOT NULL,
	`volumes` text NOT NULL,
	`socket_path` text NOT NULL,
	`timezone` text
);

CREATE INDEX `databases_database_type_idx` ON `databases` (`database_type`);
CREATE UNIQUE INDEX `databases_uuid_short_idx` ON `databases` (`uuid_short`);