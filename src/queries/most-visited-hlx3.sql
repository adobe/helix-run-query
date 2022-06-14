--- description: most visited urls within hlx3.page
--- Authorization: none
--- Cache-Control: max-age=1296000
--- fromDays: 30
--- toDays: 0
--- threshold: 100
--- limit: 100
--- version: -
WITH num_visits AS (
  SELECT
    req_url,
    COUNT(req_url) AS num_reqs,
    CONCAT(req_http_x_owner, '--', req_http_x_repo) AS repo_name
  FROM `helix_logging_7TvULgs0Xnls4q3R8tawdg.requests*`
  WHERE
    # use date partitioning to reduce query size
    _table_suffix <= CONCAT(
      CAST(EXTRACT(YEAR FROM CURRENT_TIMESTAMP()) AS String),
      LPAD(CAST(EXTRACT(MONTH FROM CURRENT_TIMESTAMP()) AS String), 2, '0')
    )
    AND _table_suffix >= CONCAT(
      CAST(
        EXTRACT(
          YEAR FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        ) AS String
      ),
      LPAD(
        CAST(
          EXTRACT(
            MONTH FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
          ) AS String
        ),
        2,
        '0'
      )
    ) AND
    (resp_http_x_version = @version OR @version = '-') AND
    resp_http_content_type LIKE 'text/html%' AND
    status_code = '200' AND
    time_start_usec > CAST(
      UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)) AS STRING
    ) AND
    time_start_usec < CAST(
      UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 0 DAY)) AS STRING
    )
  GROUP BY req_url, repo_name
),

max_visits AS (
  SELECT
    repo_name,
    MAX(num_reqs) AS max_reqs
  FROM num_visits
  GROUP BY repo_name
)

SELECT
  m.max_reqs,
  m.repo_name,
  n.req_url
FROM
  max_visits AS m,
  num_visits AS n
WHERE
  m.max_reqs > CAST(@threshold AS INT64)
  AND m.max_reqs = n.num_reqs AND
  m.repo_name = n.repo_name
ORDER BY m.max_reqs DESC
