--- description: Get page views by acquisition source for a given URL for a specified time period.
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- interval: 30
--- offset: 0
--- startdate: 2020-01-01
--- enddate: 2020-12-31
--- url: -
--- timezone: UTC
--- domainkey: secret

-- Lars wrote: We have two distinct categories:
-- Organic vs. Paid: this is based on the utm-campagin checkpoint
-- Search vs. Social vs. Direct vs. Email vs. Display: this is based on the enter checkpoint
-- And then we can build a breakdown based on the combination.

-- Julien wrote that utm data is coming

-- TODO: make a time series once the categorization is vetted

WITH events AS (
  SELECT
    hostname,
    source,
    weight,
    COUNT(source) AS count
  FROM
    helix_rum.EVENTS_V5(
      @url, # url
      CAST(@offset AS INT64), # offset
      CAST(@interval AS INT64), # days to fetch
      @startdate, # start date
      @enddate, # end date
      @timezone, # timezone
      'all', # deviceclass
      @domainkey # domain key to prevent data sharing
    )
  WHERE checkpoint = 'enter'
  GROUP BY hostname, source, weight
),

events_channels AS (
  SELECT
    source,
    COALESCE(
      -- Organic Search
      IF(source LIKE '%google%', 'Organic Search', null),
      IF(source LIKE '%duckduckgo%', 'Organic Search', null),
      IF(source LIKE '%yahoo%', 'Organic Search', null),
      IF(source LIKE '%bing%', 'Organic Search', null),
      IF(source LIKE '%ecosia%', 'Organic Search', null),
      IF(source LIKE '%explore.inblossomtab.com%', 'Organic Search', null),
      IF(source LIKE '%baidu%', 'Organic Search', null),
      IF(source LIKE '%search%', 'Organic Search', null),
      IF(source LIKE '%yandex%', 'Organic Search', null),
      -- Organic Social
      IF(source LIKE '%facebook%', 'Organic Social', null),
      IF(source LIKE '%messenger%', 'Organic Social', null),
      IF(source LIKE '%reddit%', 'Organic Social', null),
      IF(source LIKE '%justanswer%', 'Organic Social', null),
      IF(source LIKE '%pinterest%', 'Organic Social', null),
      IF(source LIKE '%linkedin%', 'Organic Social', null),
      IF(source LIKE '%tiktok%', 'Organic Social', null),
      -- Organic
      -- Paid
      IF(source LIKE '%trc.taboola.com%', 'Paid', null),
      -- Email
      IF(source LIKE '%mail3.spectrum.net%', 'Email', null),
      -- Direct
      -- TODO: investigate, direct is too high compared to GA4
      IF(source = '', 'Direct', null),
      -- everything else
      'Unassigned'
    ) AS channel,
    SUM(weight * count) AS pageviews
  FROM events
  GROUP BY source
)

SELECT
  source,
  channel,
  pageviews
FROM events_channels
ORDER BY pageviews DESC
--- source: the cleansed referer in the enter checkpoint
--- channel: the acqusition channel
--- pageviews: the number of page views in the reporting interval for each source
