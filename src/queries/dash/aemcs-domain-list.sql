--- description: Get domains with RUM originating from AEM as a Cloud Service
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- interval: 30
--- offset: 0
--- startdate: 2022-01-01
--- enddate: 2022-01-31
--- timezone: UTC
--- url: -
--- domainkey: secret

WITH hosts AS (
  SELECT
    hostname,
    REGEXP_EXTRACT(host, r'^publish-p([0-9]+)') AS program_id,
    REGEXP_EXTRACT(host, r'^publish-p[0-9]+-e([0-9]+)') AS environment_id,
    COUNT(host) AS hostname_count,
    EXTRACT(DATE FROM MIN(time) AT TIME ZONE @timezone) AS first_rum
  FROM
    helix_rum.EVENTS_V5(
      @url,
      CAST(@offset AS INT64),
      CAST(@interval AS INT64),
      @startdate,
      @enddate,
      @timezone,
      'all',
      @domainkey
    )
  WHERE
    host LIKE '%adobeaemcloud.net'
    AND host != hostname
  GROUP BY hostname, host
),

-- used for sorting, typically highest traffic env per program is production
env_events AS (
  SELECT
    environment_id,
    SUM(hostname_count) AS env_count
  FROM hosts
  GROUP BY environment_id
)

SELECT
  h.hostname,
  h.program_id,
  h.environment_id,
  h.first_rum
FROM hosts AS h
INNER JOIN env_events AS e ON h.environment_id = e.environment_id
ORDER BY CAST(h.program_id AS INT64), e.env_count DESC, h.hostname_count DESC
