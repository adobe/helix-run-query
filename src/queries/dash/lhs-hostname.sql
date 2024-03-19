--- description: Get LHS Scores for given site
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- interval: 30
--- offset: 0
--- startdate: 2022-02-01
--- enddate: 2022-05-28
--- url: -
--- domainkey: secret
SELECT * FROM `helix-225321.helix_external_data.LIGHTHOUSE_SCORES_OPT`(@url, @offset, @interval, @startdate, @enddate, @domainkey);