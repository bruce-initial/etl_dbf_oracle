SELECT
    id                            AS "ID"
  , concat(name, '-', name)       AS "NAME_NAME"
  , CAST(created_at AS timestamp) AS "CREATED_AT"
FROM
    testiles_pipeline