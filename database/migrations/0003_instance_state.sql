PRAGMA foreign_keys=OFF;
CREATE TABLE `__new_instances` (
	`uuid` blob PRIMARY KEY NOT NULL,
	`uuid_short` integer NOT NULL,
	`database_type` text NOT NULL,
	`state` text DEFAULT 'offline' NOT NULL,
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
	`root_password` text,
	`created` integer DEFAULT (unixepoch()) NOT NULL,
	CONSTRAINT "instances_state_check" CHECK("__new_instances"."state" in ('offline', 'starting', 'stopping', 'running'))
);

INSERT INTO `__new_instances`("uuid", "uuid_short", "database_type", "state", "suspended", "memory", "swap", "disk", "io_weight", "cpu", "image", "image_uid", "image_gid", "volumes", "socket_path", "timezone", "env", "cmd", "root_password", "created") SELECT "uuid", "uuid_short", "database_type", 'offline', "suspended", "memory", "swap", "disk", "io_weight", "cpu", "image", "image_uid", "image_gid", "volumes", "socket_path", "timezone", "env", "cmd", "root_password", "created" FROM `instances`;
DROP TABLE `instances`;
ALTER TABLE `__new_instances` RENAME TO `instances`;
PRAGMA foreign_keys=ON;
CREATE INDEX `instances_database_type_idx` ON `instances` (`database_type`);
CREATE UNIQUE INDEX `instances_uuid_short_idx` ON `instances` (`uuid_short`);