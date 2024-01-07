--- description: Get Daily Commits For a Site or Repo
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 30
--- offset: 0
--- startdate: 2023-02-01
--- enddate: 2023-05-28
--- timezone: UTC
--- timeunit: day
--- exactmatch: false
--- url: -
--- device: all
--- domainkey: secret
WITH current_data AS (
  SELECT *
  FROM
    `HELIX-225321.HELIX_EXTERNAL_DATA.DAILY_COMMITS`(
      @url,
      @offset,
      @interval,
      @startdate,
      @enddate,
      @domainkey
    )
)

SELECT * FROM current_data WHERE
  NOT user = 'GitHub'
  AND NOT user = 'GitHub Action'
  AND NOT user = 'GitHub Enterprise'
  AND NOT user = 'CircleCi Build'
  AND NOT user = 'Helix Bot'
  AND NOT user = 'adobe-alloy-bot'
  AND NOT user = 'github-actions'
  AND NOT user = 'github-actions[bot]'
  AND NOT user = 'helix-bot[bot]'
  AND NOT user = 'renovate[bot]'
  AND NOT user = 'semantic-release-bot'
ORDER BY owner_repo ASC, commit_date ASC
