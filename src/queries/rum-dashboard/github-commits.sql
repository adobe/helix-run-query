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
--- exactmatch: true
--- url: -
--- device: all
--- domainkey: secret
with current_data as (
SELECT 
  * 
FROM 
  helix_external_data.DAILY_COMMITS(
    @url, 
    @offset, 
    @interval, 
    @startdate,
    @enddate, 
    @domainkey
  )
) 
select * from current_data where 
not user = 'GitHub Action' 
and not user = 'GitHub Enterprise' 
and not user = 'CircleCi Build' 
and not user = 'Helix Bot' 
and not user = 'adobe-alloy-bot' 
and not user = 'github-actions' 
and not user = 'github-actions[bot]' 
and not user = 'helix-bot[bot]' 
and not user = 'renovate[bot]' 
and not user = 'semantic-release-bot'
order by owner_repo, commit_date asc

