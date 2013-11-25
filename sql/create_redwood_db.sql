CREATE TABLE IF NOT EXISTS os (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    name VARCHAR(150) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE INDEX name_UNIQUE (name ASC)
)  ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS media_source (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    reputation INT NULL,
    name VARCHAR(45) NULL,
    date_acquired DATETIME NULL,
    os_id INT UNSIGNED NOT NULL,
    PRIMARY KEY (id),
    UNIQUE INDEX name_UNIQUE (name ASC),
    CONSTRAINT fk_os_id FOREIGN KEY (os_id)
        REFERENCES os (id)
        ON DELETE NO ACTION ON UPDATE NO ACTION
)  ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS unique_file (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    hash CHAR(40) NOT NULL,
    reputation DOUBLE NOT NULL DEFAULT .5,
    PRIMARY KEY (id),
    UNIQUE INDEX hash_UNIQUE (hash ASC)
)  ENGINE=InnoDB;


CREATE TABLE IF NOT EXISTS unique_path (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    full_path VARCHAR(4096) NOT NULL,
    path_hash CHAR(40) NULL,
    PRIMARY KEY (id),
    UNIQUE INDEX path_hash_UNIQUE (path_hash ASC)
)  ENGINE=InnoDB;


CREATE TABLE IF NOT EXISTS file_metadata (
    id BIGINT UNSIGNED UNIQUE NOT NULL,
    unique_file_id BIGINT UNSIGNED NULL,
    source_id INT UNSIGNED NOT NULL,
    unique_path_id INT UNSIGNED NOT NULL,
	parent_id BIGINT UNSIGNED NULL,
    file_name VARCHAR(255) NOT NULL,
    filesystem_id MEDIUMTEXT NULL DEFAULT NULL,
    device_id INT NULL DEFAULT NULL,
    attributes INT NULL DEFAULT NULL,
    user_owner VARCHAR(45) NULL DEFAULT NULL COMMENT '	',
    group_owner VARCHAR(45) NULL DEFAULT NULL,
    size MEDIUMTEXT NULL DEFAULT NULL,
    created DATETIME NULL DEFAULT NULL,
	last_accessed DATETIME NULL DEFAULT NULL,
    last_modified DATETIME NULL DEFAULT NULL,
    last_changed DATETIME NULL DEFAULT NULL,
	user_flags INT NULL DEFAULT NULL,
    links_to_file INT NULL DEFAULT NULL,
	disk_offset BIGINT NULL,
    entropy TINYINT  NULL,
	file_content_status TINYINT NULL,
	extension VARCHAR(32) NULL,
	file_type VARCHAR(64) NULL,
    os_id INT UNSIGNED NOT NULL,
    INDEX fk_source_id_idx USING BTREE (source_id ASC),
 CONSTRAINT fk_source_id FOREIGN KEY (source_id)
        REFERENCES media_source (id)
        ON DELETE NO ACTION ON UPDATE NO ACTION,
    INDEX fk_unique_file_id_idx (unique_file_id ASC),
CONSTRAINT fk_unique_file_id FOREIGN KEY (unique_file_id)
        REFERENCES unique_file (id)
        ON DELETE NO ACTION ON UPDATE NO ACTION,
INDEX fk_unique_path_idx (unique_path_id ASC),
  CONSTRAINT fk_unique_path FOREIGN KEY (unique_path_id)
        REFERENCES unique_path (id)
        ON DELETE NO ACTION ON UPDATE NO ACTION,
    UNIQUE INDEX source_id_unique_path_id_file_name_idx USING BTREE (unique_path_id ASC , file_name ASC , source_id ASC),
    INDEX fk_os_id_idx (os_id ASC),
CONSTRAINT fk_os_id2 FOREIGN KEY (os_id)
        REFERENCES os (id)
        ON DELETE NO ACTION ON UPDATE NO ACTION,
INDEX parent_id_idx USING BTREE (parent_id ASC),
INDEX file_name_idx USING BTREE (file_name ASC)   
)  ENGINE=InnoDB;

