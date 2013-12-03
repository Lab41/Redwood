
DROP PROCEDURE IF EXISTS map_staging_table;

DELIMITER //
CREATE PROCEDURE map_staging_table(IN source_id INT, IN os_id INT)
    BEGIN
        INSERT INTO `unique_file` (hash)
                SELECT DISTINCT contents_hash
                FROM `staging_table` where dirname != "/" and LENGTH(contents_hash) > 0
        ON DUPLICATE KEY UPDATE hash = hash;
        INSERT IGNORE INTO `unique_path` (full_path, path_hash)
                SELECT dirname, dirname_hash
                FROM `staging_table`;
        INSERT INTO `file_metadata` 
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
                full_path,
                path_hash
    FROM
        file_metadata
            LEFT JOIN
        unique_file ON `file_metadata`.unique_file_id = `unique_file`.id
            LEFT JOIN
        unique_path ON `unique_path`.id = `file_metadata`.unique_path_id;


