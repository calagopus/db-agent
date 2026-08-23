ALTER TABLE `instances` ADD COLUMN `state` text DEFAULT 'offline' NOT NULL CONSTRAINT "instances_state_check" CHECK (`state` in ('offline', 'starting', 'stopping', 'running'));
