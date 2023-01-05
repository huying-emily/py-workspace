-- name: raw_import_from_star
BEGIN;

INSERT INTO masterdata.{{ master_fc_name }} (origin_term, image_type)
WITH data AS (
SELECT link_hash FROM {{ src_schema }}.{{ star_entity_name }}
EXCEPT
SELECT origin_term FROM masterdata.{{ master_fc_name }}
)
SELECT link_hash, image_type
FROM data
LEFT JOIN {{ src_schema }}.{{ star_entity_name }} USING(link_hash)
;

-- name: raw_import_by_data_source
BEGIN;

INSERT INTO masterdata.{{ master_fc_name }} (origin_term, image_type, data_source, metadata)
WITH data AS (
SELECT link_hash FROM {{ src_schema }}.{{ entity_name }}
EXCEPT
SELECT origin_term FROM masterdata.{{ master_fc_name }}
)
SELECT link_hash, image_type, '{{ data_source_value }}', {{ metadata_value }}
FROM data
LEFT JOIN {{ src_schema }}.{{ entity_name }} USING(link_hash)
;

COMMIT;

-- name: raw_direct_import_by_data_source
BEGIN;

INSERT INTO masterdata.{{ master_fc_name }} (origin_term, image_type, data_source, metadata, s3_link)
WITH data AS (
SELECT link_hash FROM {{ src_schema }}.{{ entity_name }}
EXCEPT
SELECT origin_term FROM masterdata.{{ master_fc_name }}
)
SELECT
    link_hash
    , {{ image_type_column }}
    , '{{ data_source_value }}'
    , {{ metadata_value }}
    , '{{ s3_url_prefix }}' || '/' || {{ obj_key_column }}
FROM data
LEFT JOIN {{ src_schema }}.{{ entity_name }} USING(link_hash)
;

COMMIT;


-- name: get_new_images
SELECT origin_term , b.image_type, {{ url_column }} as original_link
FROM masterdata.{{ master_fc_name }} a
LEFT JOIN {{ src_schema }}.{{ star_entity_name }} b ON a.origin_term = b.link_hash
WHERE a.s3_link ISNULL
;


-- name: get_new_images_fr_reference
SELECT origin_term , image_type, {{ url_column }} as original_link
FROM {{ src_schema }}.{{ star_entity_name }} a
-- LEFT JOIN {{ src_schema }}.{{ star_entity_name }} b ON a.origin_term = b.link_hash
WHERE type = 'image'
AND a.s3_link ISNULL
;


-- name: update_reference_image
BEGIN;

UPDATE {{ src_schema }}.{{ star_entity_name }} fc
SET s3_link = temp.s3_full_link
FROM {{ temp_table }} temp
WHERE fc.origin_term = temp.origin_term
;

COMMIT;


-- name: update_fc_image
BEGIN;

UPDATE masterdata.{{ master_fc_name }} fc
SET s3_link = temp.s3_full_link
FROM {{ temp_table }} temp
WHERE fc.origin_term = temp.origin_term
;

COMMIT;

-- name: update_s3_image_link
BEGIN;

UPDATE {{ target_schema }}.{{ entity_name }}_stored s
SET s3_uri = temp.s3_full_link
FROM {{ temp_table }} temp
WHERE s.link_hash = temp.origin_term
;

COMMIT;

-- name: fetch_new_launch
WITH avail_new_launch AS (
    SELECT project_name, vp.website AS website, id AS corrected_id
    FROM datamart.view_project vp
    LEFT JOIN raw_masterdata.sc_project sp
    ON vp.project_name = sp.name
    WHERE vp.website NOTNULL
)
SELECT al.project_name project_name, al.website website, al.corrected_id corrected_id
FROM avail_new_launch al
LEFT JOIN raw_reference.manual_project_image mpi
ON al.corrected_id = mpi.corrected_project_id
WHERE mpi.s3_link ISNULL
;
