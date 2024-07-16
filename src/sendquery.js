/*
 * Copyright 2019 Adobe. All rights reserved.
 * This file is licensed to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
import { BigQuery } from '@google-cloud/bigquery';
import size from 'json-size';
import { Response } from '@adobe/fetch';
import { auth } from './auth.js';

import {
  cleanHeaderParams,
  getHeaderParams,
  getTrailingParams,
  loadQuery,
  resolveParameterDiff,
} from './util.js';

/**
 * processes headers and request parameters
 *
 * @param {*} query
 * @param {*} params
 */
async function processParams(query, params) {
  const rawQuery = await loadQuery(query);
  const headerParams = getHeaderParams(rawQuery);
  const description = headerParams.description || '';
  const loadedQuery = rawQuery;
  const requestParams = resolveParameterDiff(
    cleanHeaderParams(loadedQuery, params),
    cleanHeaderParams(loadedQuery, headerParams),
  );
  const responseDetails = getTrailingParams(loadedQuery);
  const domainKey = requestParams.domainkey;

  return {
    headerParams,
    description,
    loadedQuery,
    requestParams,
    responseDetails,
    domainKey,
  };
}

/**
 * Log query stats to console.
 */
async function logQueryStats(job, query, domainKey, fn) {
  const [metadata] = await job.getMetadata();

  const centsPerTerabyte = 5;
  const minimumBytes = 1024 * 1024;
  const billed = parseInt(metadata.statistics.query.totalBytesBilled, 10);
  const maximumBilledBytes = Math.max(billed, billed && minimumBytes);
  const billedBytesInTerabytes = maximumBilledBytes / 1024 / 1024 / 1024 / 1024;

  const totalBilledCents = billedBytesInTerabytes * centsPerTerabyte;
  const nf = new Intl.NumberFormat('en-US', {
    style: 'unit',
    unit: 'gigabyte',
    maximumSignificantDigits: 9,
  });
  const totalBytesProcessed = parseInt(metadata.statistics.query.totalBytesProcessed, 10);
  const msg = `BigQuery job ${job.id} for `
    + `${metadata.statistics.query.cacheHit ? '(cached)' : ''} `
    + `${metadata.statistics.query.statementType} ${query} `
    + `finished with status ${metadata.status.state}, `
    + `total processed: ${nf.format(totalBytesProcessed / 1024 / 1024 / 1024)}, `
    + `total billed: ${nf.format(maximumBilledBytes / 1024 / 1024 / 1024)}, `
    + `estimated cost: ¢${totalBilledCents}, domainkey: ${domainKey}`;
  fn(msg);
}

async function loadResultsFromStream(stream) {
  return new Promise((resolve, reject) => {
    const results = [];
    let avgsize = 0;
    const maxsize = 1024 * 1024 * 6 * 0.8;

    const spaceleft = () => {
      if (results.length === 10) {
        avgsize = size(results) / results.length;
      }
      if (avgsize * results.length > maxsize) {
        return false;
      }
      return true;
    };

    let truncated = false;

    stream
      .on('data', (row) => {
        if (spaceleft()) {
          results.push(row);
        } else {
          truncated = true;
        }
      })
      .on('error', reject)
      .on('end', () => {
        resolve({ results, truncated });
      });
  });
}

/**
 * executes a query using Google Bigquery API
 *
 * @param {string} email email address of the Google service account
 * @param {string} key private key of the global Google service account
 * @param {string} project the Google project ID
 * @param {string} query the name of a .sql file in queries directory
 * @param {string} service the serviceid of the published site
 * @param {object} params parameters for substitution into query
 */
export async function execute(email, key, project, query, _, params = {}, logger = console) {
  const {
    headerParams,
    description,
    loadedQuery,
    requestParams,
    responseDetails,
    domainKey,
  } = await processParams(query, params);
  try {
    const credentials = await auth(email, key.replace(/\\n/g, '\n'));
    const bq = new BigQuery({
      projectId: project,
      credentials,
    });

    // check if dataset exists in that location

    // eslint-disable-next-line no-param-reassign
    requestParams.limit = parseInt(requestParams.limit, 10);
    const headers = cleanHeaderParams(loadedQuery, headerParams, true);
    let q = `
      IF EXISTS(
        SELECT
          *
        FROM
          helix-225321.helix_reporting.domain_keys
        WHERE
          key_bytes = SHA512(@domainkey)
          AND (revoke_date IS NULL
            OR revoke_date > CURRENT_DATE())
          AND (
            hostname_prefix = ""
            OR @url LIKE CONCAT("%.", hostname_prefix)
            OR @url LIKE CONCAT("%.", hostname_prefix, "/%")
            OR @url LIKE CONCAT(hostname_prefix)
            OR @url LIKE CONCAT(hostname_prefix, "/%")
            -- handle comma-separated list of urls, remove spaces and trailing comma
            OR hostname_prefix IN (SELECT * FROM helix_rum.URLS_FROM_LIST(@url))
          )
        )

      THEN

        ${loadedQuery}
        ;
      END IF;
    `;

    // multi-results is a special test query which does not need a domain key check
    // rorate-domainkeys is a special query which already has a domain key check
    if (query === '/multi-results' || query === '/rotate-domainkeys') {
      q = loadedQuery;
    }

    const responseMetadata = {};

    const [job] = await bq.createQueryJob({
      query: q,
      params: requestParams,
    });
    const stream = job.getQueryResultsStream();

    const { results, truncated } = await loadResultsFromStream(stream);

    const [childJobs] = await bq.getJobs({
      parentJobId: job.metadata.jobReference.jobId,
    });
    // jobs are ordered in descending order by execution time
    const statsJob = childJobs && childJobs.length >= 2 ? childJobs[1] : job;
    if (statsJob) {
      const [metadataResults] = await statsJob.getQueryResults();
      responseMetadata.totalRows = metadataResults[0]?.total_rows;
    }
    await logQueryStats(statsJob, query, domainKey, logger.info);

    return {
      headers,
      truncated,
      results,
      description,
      requestParams,
      responseDetails,
      responseMetadata,
      domainKey,
    };
  } catch (e) {
    throw new Error(`Unable to execute Google Query ${query}: ${e.message}`);
  }
}

/**
 * get query metadata
 * @param {object} params parameters for substitution into query
 */
export async function queryInfo(pathname, params) {
  const [path] = pathname.split('.');
  const {
    headerParams, description, loadedQuery, requestParams,
  } = await processParams(path, params);

  return new Response(description + Array.from(Object.entries(requestParams)).reduce((acc, [k, v]) => `${acc}  * ${k}: ${v}\n\n`, '\n'), {
    status: 200,
    headers: cleanHeaderParams(loadedQuery, headerParams, true),
  });
}
