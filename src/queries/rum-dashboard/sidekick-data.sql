--- description: Get Daily Sidekick Data From RUM for a given domain
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
WITH sidekick_events AS
(
       SELECT Format_date("%Y-%m-%d", Date_trunc(time, day)) AS day,
              id,
              checkpoint,
              hostname,
              source,
              user_agent LIKE "%Sidekick%" AS extension
       FROM   `helix-225321.helix_rum`.cluster_checkpoints( regexp_replace(@url, 'https://', ''), @offset, @interval, @startdate, @enddate, @timezone, 'all', '-' )
       WHERE  CHECKPOINT LIKE "sidekick:%"
       AND    (
                     starts_with(hostname, @url)))
SELECT   day,
         count(*)                   AS actions,
         count(DISTINCTCHECKPOINT) AS checkpoints,
FROM     sidekick_events
GROUP BY sidekick_events.day,
         hostname
ORDER BY hostname,
         day DESC