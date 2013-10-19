
DROP PROCEDURE IF EXISTS map_staging_table;

DELIMITER //
CREATE PROCEDURE map_staging_table(IN source_id INT, IN os_id INT)
    BEGIN
        INSERT INTO `unique_file` (hash)
			SELECT DISTINCT contents_hash
			FROM `staging_table`
		ON DUPLICATE KEY UPDATE prevalence_count = prevalence_count + 1;
		INSERT IGNORE INTO `unique_path` (full_path, path_hash)
			SELECT dirname, dirname_hash
			FROM `staging_table`;
		INSERT IGNORE INTO `file_metadata` 
			(id,
                        unique_file_id,
			source_id,
			unique_path_id,
			parent_id,
                        file_name,
			filesystem_id,
			device_id,
			attributes,
			user_owner,
			group_owner,
			size,
			created,
			last_accessed,
			last_modified,
			last_changed,
			user_flags,
			links_to_file,
			disk_offset,
			entropy,
			file_content_status,
			extension,
			file_type,
			os_id)
		SELECT
                        staging_table.global_file_id,
			unique_file.id,
			source_id,
			unique_path.id,
			staging_table.parent_id,
			staging_table.basename,
			staging_table.filesystem_id,
			staging_table.device_id,
			staging_table.attributes,
			staging_table.user_owner,
			staging_table.group_owner,
			staging_table.size,
			staging_table.created,
			staging_table.last_accessed,
			staging_table.last_modified,
			staging_table.last_changed,
			staging_table.user_flags,
			staging_table.links_to_file,
			staging_table.disk_offset,
			staging_table.entropy,
			staging_table.file_content_status,
			staging_table.extension,
			staging_table.file_type,
			os_id
		FROM `staging_table`
			LEFT JOIN  `unique_file`
			ON (staging_table.contents_hash = unique_file.hash)
			LEFT JOIN `unique_path`
			ON (staging_table.dirname_hash = unique_path.path_hash);
    END //
DELIMITER ;

DROP PROCEDURE IF EXISTS get_hash_list_by_prevalence;

DELIMITER //
CREATE PROCEDURE get_hash_list_by_prevalence(IN min_percent INT)
    BEGIN
		SET @source_count := (SELECT COUNT(id) FROM media_source);
		SELECT 
			hash 
		FROM unique_file 
		WHERE (prevalence_count / @source_count * 100) >= min_percent;
    END //
DELIMITER ;

DROP PROCEDURE IF EXISTS recompute_prevalence_score;

DELIMITER //
CREATE PROCEDURE recompute_prevalence_score(IN target_os VARCHAR(150))
    BEGIN
		SET @os_count := (SELECT COUNT(id) FROM media_source WHERE os = target_os);

		UPDATE unique_file
		SET unique_file.prevalence_score = (SELECT (COUNT(DISTINCT hash, media_source.id) / @os_count)
		FROM file_metadata 
			LEFT JOIN media_source
			ON (file_metadata.source_id = media_source.id)
		WHERE media_source.os = target_os AND unique_file.id = file_metadata.unique_file_id
		GROUP BY hash, os);
    END //
DELIMITER ;

DROP VIEW IF EXISTS joined_file_metadata;

CREATE VIEW `joined_file_metadata` AS
    SELECT 
        `file_metadata`.id AS file_metadata_id,
		unique_file_id,
		source_id,
		unique_path_id,
		file_name,
		parent_id,
		filesystem_id,
		device_id,
		attributes,
		user_owner,
		group_owner,
		size,
		created,
		last_accessed,
		last_modified,
		last_changed,
		user_flags,
		links_to_file,
		disk_offset,
		entropy,
		file_content_status,
		extension,
		file_type,
		hash,
		reputation,
		prevalence_count,
		full_path,
		path_hash
    FROM
        file_metadata
            LEFT JOIN
        unique_file ON `file_metadata`.unique_file_id = `unique_file`.id
            LEFT JOIN
        unique_path ON `unique_path`.id = `file_metadata`.unique_path_id;

DROP VIEW IF EXISTS distinct_files_by_directory;

CREATE VIEW `distinct_files_by_directory` AS
    SELECT DISTINCT
            unique_file.id 'unique_file_id',
unique_path.full_path,
unique_path.id 'unique_path_id',
unique_file.prevalence_count
        FROM
            unique_file
        INNER JOIN file_metadata ON (unique_file.id = file_metadata.unique_file_id)
        INNER JOIN unique_path ON (file_metadata.unique_path_id = unique_path.id);

CREATE VIEW `directory_average_file_prevalence` AS
    SELECT
        distinct_files_by_directory.full_path,
        distinct_files_by_directory.unique_path_id,
COUNT(distinct_files_by_directory.unique_path_id),
        AVG(distinct_files_by_directory.prevalence_count) 'prevalence'
    FROM
        distinct_files_by_directory
    GROUP BY distinct_files_by_directory.full_path;
