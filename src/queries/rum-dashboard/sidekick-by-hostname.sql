WITH sidekick_events AS
(
       SELECT Format_date("%Y-%m-%d", Date_trunc(time, day)) AS day,
              id,
              checkpoint,
              hostname,
              source,
              user_agent LIKE "%Sidekick%" AS extension
       FROM   helix_rum.CHECKPOINTS_V4(@url, @offset, @interval, @startdate, @enddate, @timezone, 'all', @domainkey )
       WHERE  CHECKPOINT LIKE "sidekick:%")
SELECT   day,
         count(*)                   AS actions,
         count(DISTINCT CHECKPOINT) AS checkpoints,
FROM     sidekick_events
GROUP BY sidekick_events.day,
         hostname
ORDER BY hostname,
         day DESC