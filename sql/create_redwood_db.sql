
CREATE TABLE IF NOT EXISTS `media_source` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `reputation` INT NULL,
    `name` VARCHAR(45) NULL,
    `date_acquired` DATETIME NULL,
    `os` VARCHAR(150) NULL,
    PRIMARY KEY (`id`),
    UNIQUE INDEX `name_UNIQUE` (`name` ASC)
)  ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `unique_file` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `hash` CHAR(40) NOT NULL,
    `reputation` DOUBLE NOT NULL DEFAULT .5,
    `prevalence_count` INT NULL DEFAULT 1,
	`prevalence_score` FLOAT NULL DEFAULT 0,
    `locality_uniqueness_score` DOUBLE NULL,
    PRIMARY KEY (`id`),
    UNIQUE INDEX `hash_UNIQUE` (`hash` ASC)
)  ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `unique_path` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `full_path` VARCHAR(4096) NOT NULL,
    `path_hash` CHAR(40) NULL,
    PRIMARY KEY (`id`),
    UNIQUE INDEX `path_hash_UNIQUE` (`path_hash` ASC)
)  ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `file_metadata` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `unique_file_id` INT NOT NULL,
    `source_id` INT NOT NULL,
    `unique_path_id` INT NOT NULL,
    `file_name` VARCHAR(255) NOT NULL,
    `inode` MEDIUMTEXT NULL,
    `device_id` INT NULL,
    `permissions` INT NULL,
    `user_owner` VARCHAR(45) NULL COMMENT '	',
    `group_owner` VARCHAR(45) NULL,
    `last_accessed` DATETIME NULL,
    `last_modified` DATETIME NULL,
    `last_changed` DATETIME NULL,
    `created` DATETIME NULL,
    `user_flags` INT NULL,
    `links_to_file` INT NULL,
    `size` MEDIUMTEXT NULL,
    INDEX `fk_source_id_idx` USING BTREE (`source_id` ASC),
    INDEX `fk_unique_file_id_idx` (`unique_file_id` ASC),
    PRIMARY KEY (`id`),
    UNIQUE INDEX `source_id_unique_path_id_file_name_idx` USING BTREE (`unique_path_id` ASC , `file_name` ASC , `source_id` ASC),
    INDEX `file_name_idx` USING BTREE (`file_name` ASC),
    INDEX `fk_unique_path_idx` (`unique_path_id` ASC),
    CONSTRAINT `fk_source_id` FOREIGN KEY (`source_id`)
        REFERENCES `media_source` (`id`)
        ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT `fk_unique_file_id` FOREIGN KEY (`unique_file_id`)
        REFERENCES `unique_file` (`id`)
        ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT `fk_unique_path` FOREIGN KEY (`unique_path_id`)
        REFERENCES `unique_path` (`id`)
        ON DELETE NO ACTION ON UPDATE NO ACTION
)  ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `staging_table` (
    `hash` CHAR(40) NULL,
    `full_path` VARCHAR(4096) NULL,
    `path_hash` CHAR(40) NULL,
    `file_name` VARCHAR(255) NULL,
    `inode` MEDIUMTEXT NULL,
    `device_id` INT NULL,
    `permissions` INT NULL,
    `user_owner` VARCHAR(45) NULL COMMENT '	',
    `group_owner` VARCHAR(45) NULL,
    `last_accessed` BIGINT NULL,
    `last_modified` BIGINT NULL,
    `last_changed` BIGINT NULL,
    `created` BIGINT NULL,
    `user_flags` INT NULL,
    `links_to_file` INT NULL,
    `size` MEDIUMTEXT NULL
)  ENGINE=InnoDB;
