/*
 * Copyright 2021 Adobe. All rights reserved.
 * This file is licensed to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
*/
--- description: most served blobs for helix pages
--- Authorization: none
--- Cache-Control: max-age=60
--- fromHours: 720
--- status: 200
--- limit: 100
--- offset: 0
SELECT * FROM (
SELECT
    IF(req_http_X_Owner != "", req_http_X_Owner, REGEXP_EXTRACT(req_http_host, "[^\\-\\.]+--([^\\-\\.]+)\\.", 1))  AS owner,
    IF(req_http_X_Repo != "", req_http_X_Repo,
        IF(
            ARRAY_LENGTH(SPLIT(SPLIT(req_http_host, '.')[OFFSET(0)], '--')) = 3,
            SPLIT(SPLIT(req_http_host, '.')[OFFSET(0)], '--')[OFFSET(1)],
            SPLIT(SPLIT(req_http_host, '.')[OFFSET(0)], '--')[OFFSET(0)]
                ))AS repo,
    REGEXP_EXTRACT(req_http_X_URL, "_([0-9a-f]+)\\.[a-z]+\\??", 1) AS id,
    REGEXP_EXTRACT(req_http_X_URL, "_[0-9a-f]+\\.([a-z]+)\\??", 1) AS format,
    TIMESTAMP_MICROS(CAST(MAX(time_start_usec) AS INT64)) AS latestreq,
    TIMESTAMP_MICROS(CAST(MIN(time_start_usec) AS INT64)) AS earliestreq,
    COUNT(time_start_usec) AS requests,
    MAX(req_http_host) AS host,
FROM `helix_logging_7TvULgs0Xnls4q3R8tawdg.requests*`
WHERE req_http_X_URL LIKE "%_media%"
    AND status_code = CAST(@status AS STRING) AND
    _TABLE_SUFFIX <= CONCAT(CAST(EXTRACT(YEAR FROM CURRENT_TIMESTAMP()) AS String), LPAD(CAST(EXTRACT(MONTH FROM CURRENT_TIMESTAMP()) AS String), 2, "0")) AND
    _TABLE_SUFFIX >= CONCAT(CAST(EXTRACT(YEAR FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @fromHours HOUR)) AS String), LPAD(CAST(EXTRACT(MONTH FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @fromHours HOUR)) AS String), 2, "0")) AND
    time_start_usec > CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @fromHours HOUR)) AS STRING) AND
    time_start_usec < CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 0 DAY)) AS STRING)
GROUP BY owner, repo, id, format
ORDER BY requests DESC
)
WHERE id IS NOT NULL AND format IS NOT NULL
LIMIT @limit OFFSET @offset
