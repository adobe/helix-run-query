--- description: List of domains along with cs and some summary data.
--- Access-Control-Allow-Origin: *
--- Cache-Control: max-age=3600
--- timezone: UTC
--- device: all
--- domainkey: secret
--- interval: 365
--- offset: 0
--- startdate: 2022-02-01
--- enddate: 2022-05-28
--- url: -

WITH pvs AS (
  SELECT
    host,
    SUM(pageviews) AS pageviews,
    REGEXP_REPLACE(hostname, r'^www.', '') AS hostname,
    FORMAT_DATE('%Y-%b', time) AS month,
    MIN(time) AS first_visit,
    MAX(time) AS last_visit
  FROM
    helix_rum.PAGEVIEWS_V5(
      @url,
      CAST(@offset AS INT64),
      CAST(@interval AS INT64),
      @startdate,
      @enddate,
      @timezone,
      @device,
      @domainkey
    )
  WHERE
    hostname != ''
    AND NOT REGEXP_CONTAINS(hostname, r'^\d+\.\d+\.\d+\.\d+$') -- IP addresses
    AND hostname NOT LIKE 'localhost%'
    AND hostname NOT LIKE '%.hlx.page'
    AND hostname NOT LIKE '%.hlx3.page'
    AND hostname NOT LIKE '%.hlx.live'
    AND hostname NOT LIKE '%.helix3.dev'
    AND hostname NOT LIKE '%.sharepoint.com'
    OR hostname = 'www.hlx.live'
  GROUP BY month, host, hostname
),

cs AS (
  SELECT DISTINCT
    hostname,
    REGEXP_EXTRACT(host, r'^publish-p([0-9]+)') AS program_id,
    REGEXP_EXTRACT(host, r'^publish-p[0-9]+-e([0-9]+)') AS environment_id
  FROM pvs
  WHERE REGEXP_CONTAINS(host, r'^publish-p[0-9]+-e[0-9]+')
),

total_pvs AS (
  SELECT
    hostname,
    FORMAT_DATE('%F', MIN(first_visit)) AS first_visit,
    FORMAT_DATE('%F', MAX(last_visit)) AS last_visit,
    SUM(pageviews) AS estimated_pvs
  FROM pvs
  GROUP BY hostname
),

domains AS (
  SELECT
    a.hostname,
    a.first_visit,
    a.last_visit,
    SUM(b.pageviews) AS current_month_visits,
    SUM(a.estimated_pvs) AS total_visits
  FROM total_pvs AS a
  LEFT JOIN
    pvs AS b
    ON
      a.hostname = b.hostname AND b.month = FORMAT_DATE('%Y-%b', CURRENT_DATE())
  GROUP BY
    a.hostname, a.first_visit, a.last_visit
)

SELECT
  a.hostname,
  '' AS ims_org_id,
  a.first_visit,
  a.last_visit,
  a.current_month_visits,
  a.total_visits,
  CAST(cs.program_id AS INT64) AS program_id,
  CAST(cs.environment_id AS INT64) AS environment_id
FROM domains AS a
LEFT JOIN cs ON a.hostname = cs.hostname
WHERE
  DATE(a.last_visit) > (CURRENT_DATE() - 60)
ORDER BY a.total_visits DESC, a.current_month_visits DESC
