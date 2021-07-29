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
const { BigQuery } = require('@google-cloud/bigquery');
const size = require('json-size');
const { auth } = require('./auth.js');
const { Response } = require('@adobe/helix-universal');
const {
  loadQuery, replaceTableNames, cleanHeaderParams,
  cleanQuery, getHeaderParams, authFastly,
  resolveParameterDiff,
} = require('./util.js');

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

  return {
    headerParams,
    description,
    loadedQuery,
    requestParams,
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
async function execute(email, key, project, query, service, params = {}) {
  const {
    headerParams,
    description,
    loadedQuery,
    requestParams,
  } = await processParams(query, params);

  if (headerParams && headerParams.Authorization === 'fastly') {
    try {
      await authFastly(params.token, params.service);
    } catch (e) {
      e.statusCode = 401;
      throw e;
    }
  }
  delete headerParams.Authorization;
  try {
    const credentials = await auth(email, key.replace(/\\n/g, '\n'));
    const bq = new BigQuery({
      projectId: project,
      credentials,
    });
    const [dataset] = await bq.dataset(`helix_logging_${service}`, {
      location: 'US',
    }).get();

    // eslint-disable-next-line no-async-promise-executor
    return new Promise((resolve, reject) => {
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

      replaceTableNames(loadedQuery, {
        myrequests: () => `SELECT * FROM \`${dataset.id}.requests*\``,
        allrequests: async () => {
          const [alldatasets] = await bq.getDatasets();
          return alldatasets
            .filter(({ id }) => id.match(/^helix_logging_*/g))
            .map(({ id }) => id)
            .map((id) => `SELECT * FROM \`${id}.requests*\``)
            .join(' UNION ALL\n');
        },
      }).then((q) => {
        dataset.createQueryStream({
          query: q,
          maxResults: params.limit,
          params: requestParams,
        })
          .on('data', (row) => (spaceleft() ? results.push(row) : resolve({
            headers,
            truncated: true,
            results,
            description,
            requestParams,
          })))
          .on('error', (e) => reject(e))
          .on('end', () => resolve({
            headers,
            truncated: false,
            results,
            description,
            requestParams,
          }));
      });
    });
  } catch (e) {
    throw new Error(`Unable to execute Google Query: ${e.message}`);
  }
}

/**
 * get query metadata
 * @param {object} params parameters for substitution into query
 */
async function queryInfo(pathname, params) {
  const [ path ] = pathname.split('.');
  const {
    headerParams, description, loadedQuery, requestParams,
  } = await processParams(path, params);

  return new Response(JSON.stringify({
    text: description,
    requestParams
  }), {
    status: 200,
    headers: cleanHeaderParams(loadedQuery, headerParams, true)
  });
}

module.exports = { execute, queryInfo };
