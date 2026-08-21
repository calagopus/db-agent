CREATE TABLE `__user_database_links` AS
SELECT `uuid` AS `user_uuid`, `database_uuid`, `created` FROM `users` WHERE `database_uuid` IS NOT NULL;
CREATE TABLE `__new_users` (
	`uuid` blob PRIMARY KEY NOT NULL,
	`uuid_short` integer NOT NULL,
	`instance_uuid` blob NOT NULL,
	`username` text NOT NULL,
	`password` text NOT NULL,
	`created` integer DEFAULT (unixepoch()) NOT NULL,
	FOREIGN KEY (`instance_uuid`) REFERENCES `instances`(`uuid`) ON UPDATE no action ON DELETE cascade
);

INSERT INTO `__new_users`("uuid", "uuid_short", "instance_uuid", "username", "password", "created") SELECT "uuid", "uuid_short", "instance_uuid", "username", "password", "created" FROM `users`;
DROP TABLE `users`;
ALTER TABLE `__new_users` RENAME TO `users`;
CREATE INDEX `users_instance_uuid_idx` ON `users` (`instance_uuid`);
CREATE INDEX `users_username_idx` ON `users` (`username`);
CREATE UNIQUE INDEX `users_uuid_short_idx` ON `users` (`uuid_short`);

CREATE TABLE `user_databases` (
	`user_uuid` blob NOT NULL,
	`database_uuid` blob NOT NULL,
	`permission` text NOT NULL,
	`created` integer DEFAULT (unixepoch()) NOT NULL,
	PRIMARY KEY(`user_uuid`, `database_uuid`),
	FOREIGN KEY (`user_uuid`) REFERENCES `users`(`uuid`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`database_uuid`) REFERENCES `databases`(`uuid`) ON UPDATE no action ON DELETE cascade,
	CONSTRAINT "user_databases_permission_check" CHECK("user_databases"."permission" in ('read_only', 'read_write'))
);

CREATE INDEX `user_databases_database_uuid_idx` ON `user_databases` (`database_uuid`);

INSERT INTO `user_databases` ("user_uuid", "database_uuid", "permission", "created")
SELECT "user_uuid", "database_uuid", 'read_write', "created" FROM `__user_database_links`;

DROP TABLE `__user_database_links`;
