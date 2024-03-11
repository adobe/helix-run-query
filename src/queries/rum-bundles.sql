--- description: get raw rum data for a given day and base URL
--- Cache-Control: max-age=86400
--- url: -
--- startdate: 2024-01-01
--- enddate: 2024-01-02
--- domainkey: secret

SELECT
  id,
  STRING(MIN(time)) AS time, -- noqa: RF04
  CAST(ANY_VALUE(weight) AS INT64) AS weight,
  ARRAY_AGG(
    STRUCT(
      STRING(time) AS time, -- noqa: RF04
      url,
      CASE
        WHEN LCP IS NOT NULL THEN 'cwv-lcp'
        WHEN INP IS NOT NULL THEN 'cwv-inp'
        WHEN CLS IS NOT NULL THEN 'cwv-cls'
        WHEN TTFB IS NOT NULL THEN 'cwv-ttfb'
        ELSE checkpoint
      END AS checkpoint,
      source,
      target,
      CASE
        WHEN LCP IS NOT NULL THEN LCP
        WHEN INP IS NOT NULL THEN INP
        WHEN CLS IS NOT NULL THEN CLS
        WHEN TTFB IS NOT NULL THEN TTFB
      END AS value, -- noqa: RF04
      user_agent
    )
  ) AS events
FROM
  helix_rum.EVENTS_V5(
    @url,
    -1,
    -1,
    @startdate,
    # startdate plus one day
    FORMAT_TIMESTAMP(
      "%Y-%m-%d",
      TIMESTAMP_ADD(TIMESTAMP(@startdate), INTERVAL 1 DAY)
    ),
    "UTC",
    "all",
    @domainkey
  )
GROUP BY
  id,
  TIMESTAMP_TRUNC(time, DAY)
