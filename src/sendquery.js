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
  cleanQuery,
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
  const loadedQuery = cleanQuery(rawQuery);
  const requestParams = resolveParameterDiff(
    cleanHeaderParams(loadedQuery, params),
    cleanHeaderParams(loadedQuery, headerParams),
  );
  const responseDetails = getTrailingParams(loadedQuery);

  return {
    headerParams,
    description,
    loadedQuery,
    requestParams,
    responseDetails,
  };
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
export async function execute(email, key, project, query, _, params = {}) {
  const {
    headerParams,
    description,
    loadedQuery,
    requestParams,
    responseDetails,
  } = await processParams(query, params);
  try {
    const credentials = await auth(email, key.replace(/\\n/g, '\n'));
    const bq = new BigQuery({
      projectId: project,
      credentials,
    });

    // check if dataset exists in that location

    // eslint-disable-next-line no-async-promise-executor
    return new Promise(async (resolve, reject) => {
      const results = [];
      let avgsize = 0;
      const maxsize = 1024 * 1024 * 0.9;
      // eslint-disable-next-line no-param-reassign
      requestParams.limit = parseInt(requestParams.limit, 10);
      const headers = cleanHeaderParams(loadedQuery, headerParams, true);

      const spaceleft = () => {
        if (results.length === 10) {
          avgsize = size(results) / results.length;
        }
        if (avgsize * results.length > maxsize) {
          return false;
        }
        return true;
      };

      const q = loadedQuery;
      const stream = await bq.createQueryStream({
        query: q,
        maxResults: params.limit,
        params: requestParams,
      });
      stream
        .on('data', (row) => (spaceleft() ? results.push(row) : resolve({
          headers,
          truncated: true,
          results,
          description,
          requestParams,
          responseDetails,
        })))
        .on(
          'error',
          /* c8 ignore next 3 */
          async (e) => {
            reject(e);
          },
        )
        .on('end', async () => {
          resolve({
            headers,
            truncated: false,
            results,
            description,
            requestParams,
            responseDetails,
          });
        });
    });
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
