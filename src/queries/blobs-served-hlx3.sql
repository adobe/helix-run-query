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
        REQ_HTTP_X_URL AS URL,
        IF(
            REQ_HTTP_X_OWNER != "",
            REQ_HTTP_X_OWNER,
            REGEXP_EXTRACT(REQ_HTTP_HOST, "[^\\-\\.]+--([^\\-\\.]+)\\.", 1)
        ) AS OWNER,
        IF(REQ_HTTP_X_REPO != "", REQ_HTTP_X_REPO,
            IF(
                ARRAY_LENGTH(
                    SPLIT(SPLIT(REQ_HTTP_HOST, ".") [OFFSET(0)], "--")
                ) = 3,
                SPLIT(SPLIT(REQ_HTTP_HOST, ".") [OFFSET(0)], "--") [OFFSET(1)],
                SPLIT(SPLIT(REQ_HTTP_HOST, ".") [OFFSET(0)], "--") [OFFSET(0)]
            )) AS REPO,
        REGEXP_EXTRACT(REQ_HTTP_X_URL, "_([0-9a-f]+)\\.[a-z]+\\??", 1) AS ID,
        REGEXP_EXTRACT(
            REQ_HTTP_X_URL, "_[0-9a-f]+\\.([a-z]+)\\??", 1
        ) AS FORMAT,
        TIMESTAMP_MICROS(CAST(MAX(TIME_START_USEC) AS INT64)) AS LATESTREQ,
        TIMESTAMP_MICROS(CAST(MIN(TIME_START_USEC) AS INT64)) AS EARLIESTREQ,
        COUNT(TIME_START_USEC) AS REQUESTS,
        MAX(REQ_HTTP_HOST) AS HOST
    FROM `helix_logging_7TvULgs0Xnls4q3R8tawdg.requests*`
    WHERE REQ_HTTP_X_URL LIKE "%_media%"
        AND STATUS_CODE = CAST(@status AS STRING)
        AND _TABLE_SUFFIX <= CONCAT(
            CAST(EXTRACT(YEAR FROM CURRENT_TIMESTAMP()) AS String),
            LPAD(
                CAST(EXTRACT(MONTH FROM CURRENT_TIMESTAMP()) AS String), 2, "0"
            )
        )
        AND _TABLE_SUFFIX >= CONCAT(
            CAST(
                EXTRACT(
                    YEAR FROM TIMESTAMP_SUB(
                        CURRENT_TIMESTAMP(), INTERVAL @fromHours HOUR
                    )
                ) AS String
            ),
            LPAD(
                CAST(
                    EXTRACT(
                        MONTH FROM TIMESTAMP_SUB(
                            CURRENT_TIMESTAMP(), INTERVAL @fromHours HOUR
                        )
                    ) AS String
                ),
                2,
                "0"
            )
        )
        AND TIME_START_USEC > CAST(
            UNIX_MICROS(
                TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @fromHours HOUR)
            ) AS STRING
        )
        AND TIME_START_USEC < CAST(
            UNIX_MICROS(
                TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 0 DAY)
            ) AS STRING
        )
    GROUP BY OWNER, REPO, ID, FORMAT, URL
    ORDER BY REQUESTS DESC
)
WHERE ID IS NOT NULL AND FORMAT IS NOT NULL
LIMIT @limit OFFSET @offset
