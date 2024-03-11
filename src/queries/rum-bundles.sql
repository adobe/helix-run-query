--- description: get raw rum data for a given day and base URL
--- Cache-Control: max-age=86400
--- url: -
--- startdate: 2024-01-01
--- enddate: 2024-01-02
--- domainkey: secret

SELECT
  id,
  time,
  url,
  checkpoint,
  source,
  target,
  weight,
  user_agent,
  LCP,
  INP,
  CLS,
  TTFB
FROM helix_rum.EVENTS_V5(
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
