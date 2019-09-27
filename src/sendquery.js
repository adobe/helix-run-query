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
const fs = require('fs-extra');
const path = require('path');
const size = require('json-size');
const { auth } = require('./auth.js');
const QueryExecutionError = require('./QueryExecutionError.js');

class QueryLoadingError extends Error {
  constructor(message) {
    super(message);
    this.statusCode = 404;
  }
}

function loadQuery(query) {
  return fs.readFileSync(path.resolve(__dirname, 'queries', `${query.replace(/^\//, '')}.sql`)).toString();
}

/**
 *
 * @param {string} email email address of the Google service account
 * @param {string} key private key of the global Google service account
 * @param {string} project the Google project ID
 * @param {string} query the query from a .sql file
 */
async function execute(email, key, project, queryname, service, params = {
  limit: 100,
}) {
  try {
    const query = loadQuery(queryname);

    const credentials = await auth(email, key);

    const bq = new BigQuery({
      projectId: project,
      credentials,
    });
    const [dataset] = await bq.dataset(`helix_logging_${service}`, {
      location: 'US',
    }).get();

    return new Promise((resolve, reject) => {
      const results = [];
      let avgsize = 0;

      const spaceleft = () => {
        if (results.length === 10) {
          avgsize = size(results) / results.length;
        }
        if (avgsize * results.length > 1024 * 1024 * 0.9) {
          return false;
        }
        return true;
      };

      dataset.createQueryStream({
        query,
        maxResults: parseInt(params.limit, 10),
        params,
      })
        .on('data', (row) => (spaceleft() ? results.push(row) : resolve({
          truncated: true,
          results,
        })))
        .on('error', (e) => reject(e))
        .on('end', () => resolve({
          truncated: false,
          results,
        }));
    });
  } catch (e) {
    if (e.code && e.code === 'ENOENT') {
      throw new QueryLoadingError('Query is not supported');
    }
    throw new QueryExecutionError(`Unable to execute Google Query: ${e.message}`);
  }
}

module.exports = {
  execute, loadQuery, QueryExecutionError, QueryLoadingError,
};
