--- description: Get bounce rate and estimated session length (in pages) for a given site. A bounce is defined as a visitor who enters the page without clicking anywhere.
--- interval: 30
--- offset: 0
--- startdate: 2022-01-01
--- enddate: 2022-01-31
--- timezone: UTC
--- url: -
--- domainkey: secret
WITH current_data AS (
  SELECT
    id,
    hostname,
    checkpoint,
    source,
    target,
    pageviews
  FROM
    helix_rum.CHECKPOINTS_V3(
      @url,
      CAST(@offset AS INT64),
      CAST(@interval AS INT64),
      @startdate,
      @enddate,
      @timezone,
      'all',
      @domainkey
    )
),

# Get the number of pages viewed per session
pageviews_per_session AS (
  SELECT
    hostname,
    CAST(source AS INT64) AS pagesseen,
    ANY_VALUE(pageviews) AS pageviews
  FROM current_data
  WHERE checkpoint = 'pagesviewed'
  GROUP BY
    id,
    hostname,
    pagesseen
),

# Get the number of sessions per hostname
# we estimate the length of the session by
# taking the mean number of pages seen, then
# adjust for right-censoring the data through sampling
sessions AS (
  SELECT
    hostname,
    SUM(pageviews) AS pageviews,
    2 * (AVG(pagesseen) - 1) AS pagespervisit,
    SUM(pageviews) / (2 * (AVG(pagesseen) - 1)) AS visits
  FROM pageviews_per_session
  GROUP BY
    hostname
),

enter_sessions AS (
  SELECT id
  FROM current_data
  WHERE checkpoint = 'enter'
  GROUP BY
    id
),

click_sessions AS (
  SELECT id
  FROM current_data
  WHERE
    checkpoint = 'click'
    AND target IS NOT NULL
  GROUP BY
    id
),

enter_click_sessions AS (
  SELECT enter_sessions.id
  FROM enter_sessions
  INNER JOIN click_sessions ON enter_sessions.id = click_sessions.id
),

bounce_rate AS (
SELECT
  0 AS all_enter_click_sessions,
  COUNT(*) AS all_enter_sessions
  FROM enter_sessions
  UNION ALL
SELECT
  0 AS all_enter_sessions,
  COUNT(*) AS all_enter_click_sessions
  FROM enter_click_sessions
),

aggregate AS (
  SELECT
    SUM(pageviews) AS pageviews,
    SUM(visits) AS visits,
    AVG(pagespervisit) AS pagespervisit,
    0 AS all_enter_sessions,
    0 AS all_enter_click_sessions
  FROM sessions
  UNION ALL
  SELECT
    0 AS pageviews,
    0 AS visits,
    0 AS pagespervisit,
    all_enter_sessions,
    all_enter_click_sessions
  FROM bounce_rate
)

SELECT
  MAX(pageviews) AS pageviews,
  MAX(visits) AS visits,
  MAX(pagespervisit) AS pagespervisit,
  MAX(all_enter_click_sessions) / MAX(all_enter_sessions) AS bounce_rate
FROM aggregate
--- pageviews: number of page views in the time period
--- visits: number of visits in the time period, based on estimated session length
--- pagespervisit: estimated session length in pages per visit
--- bounce_rate: fraction of visitors who bounce, i.e. enter the page without clicking anywhere
