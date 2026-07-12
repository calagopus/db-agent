CREATE TABLE `databases` (
	`uuid` blob PRIMARY KEY NOT NULL,
	`instance_uuid` blob NOT NULL,
	`name` text NOT NULL,
	`created` integer DEFAULT (unixepoch()) NOT NULL,
	FOREIGN KEY (`instance_uuid`) REFERENCES `instances`(`uuid`) ON UPDATE no action ON DELETE cascade
);

CREATE INDEX `databases_instance_uuid_idx` ON `databases` (`instance_uuid`);
CREATE UNIQUE INDEX `databases_instance_uuid_name_idx` ON `databases` (`instance_uuid`,`name`);
CREATE TABLE `instances` (
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
	`timezone` text,
	`env` text DEFAULT '{}' NOT NULL,
	`cmd` text,
	`created` integer DEFAULT (unixepoch()) NOT NULL
);

CREATE INDEX `instances_database_type_idx` ON `instances` (`database_type`);
CREATE UNIQUE INDEX `instances_uuid_short_idx` ON `instances` (`uuid_short`);
CREATE TABLE `users` (
	`uuid` blob PRIMARY KEY NOT NULL,
	`uuid_short` integer NOT NULL,
	`instance_uuid` blob NOT NULL,
	`database_uuid` blob,
	`username` text NOT NULL,
	`password` text NOT NULL,
	`created` integer DEFAULT (unixepoch()) NOT NULL,
	FOREIGN KEY (`instance_uuid`) REFERENCES `instances`(`uuid`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`database_uuid`) REFERENCES `databases`(`uuid`) ON UPDATE no action ON DELETE cascade
);

CREATE INDEX `users_instance_uuid_idx` ON `users` (`instance_uuid`);
CREATE INDEX `users_database_uuid_idx` ON `users` (`database_uuid`);
CREATE INDEX `users_username_idx` ON `users` (`username`);
CREATE UNIQUE INDEX `users_uuid_short_idx` ON `users` (`uuid_short`);