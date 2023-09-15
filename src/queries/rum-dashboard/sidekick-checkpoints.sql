--- description: Get All Sidekick Checkpoints From RUM for a given domain
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 30
--- offset: 0
--- startdate: 2023-02-01
--- enddate: 2023-05-28
--- timezone: UTC
--- url: -
--- device: all
--- domainkey: secret
with sidekick_events AS (
SELECT
  checkpoint,
  hostname
FROM   helix_rum.CHECKPOINTS_V4(@url, @offset, @interval, @startdate, @enddate, @timezone, 'all', @domainkey )
WHERE  CHECKPOINT LIKE "sidekick:%")
SELECT 
     CHECKPOINT,
FROM     sidekick_events
GROUP BY checkpoint
ORDER BY checkpoint desc