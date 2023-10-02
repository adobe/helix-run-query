--- description: List of domains along with some summary data.
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
    SUM(pageviews) AS pageviews,
    REGEXP_REPLACE(hostname, r'^www.', '') AS hostname,
    FORMAT_DATE('%Y-%b', time) AS month,
    MIN(time) AS first_visit,
    MAX(time) AS last_visit
  FROM
    helix_rum.PAGEVIEWS_V3(
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
    AND hostname NOT LIKE '%.hlx%.live'
    AND hostname NOT LIKE '%.helix3.dev'
    AND hostname NOT LIKE '%.sharepoint.com'
    AND hostname NOT LIKE '%.google.com'
    AND hostname NOT LIKE '%.edison.pfizer' -- not live
    AND hostname NOT LIKE '%.web.pfizer'
    AND hostname NOT LIKE 'author-p%-e%'
    OR hostname = 'www.hlx.live'
  GROUP BY month, hostname
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
    b.pageviews AS current_month_visits,
    a.estimated_pvs AS total_visits
  FROM total_pvs AS a
  LEFT JOIN
    pvs AS b
    ON
      a.hostname = b.hostname AND b.month = FORMAT_DATE('%Y-%b', CURRENT_DATE())
  GROUP BY
    a.hostname, a.first_visit, a.last_visit, a.estimated_pvs, b.pageviews
),

repos AS (
  SELECT
    a.url,
    a.live,
    a.owner_repo,
    a.requests,
    MIN(b.commit_date) AS first_commit
  FROM
    helix_admin_data.gh_public_domain_linkage AS a
  LEFT JOIN
    helix_external_data.github_commits AS b
    ON
      a.owner_repo = b.owner_repo
  GROUP BY a.url, a.live, a.owner_repo, a.requests
  ORDER BY a.url
)

SELECT
  a.hostname AS domain_name,
  a.first_visit AS go_live,
  IF(COUNT(DISTINCT b.owner_repo) > 0, 'edge', 'other') AS domain_type,
  IF(
    DATE(a.last_visit) > (CURRENT_DATE() - 30),
    'active',
    'inactive'
  ) AS domain_status,
  ARRAY(
    SELECT DISTINCT owner_repo FROM repos WHERE a.hostname = url
  ) AS git_repo
FROM domains AS a
LEFT JOIN
  repos AS b
  ON
    a.hostname = b.url
WHERE
  a.total_visits >= 1000
  AND DATE(a.last_visit) > (CURRENT_DATE() - 365)
GROUP BY
  a.hostname,
  a.last_visit,
  a.first_visit,
  a.total_visits,
  a.current_month_visits
ORDER BY a.total_visits DESC, a.current_month_visits DESC
