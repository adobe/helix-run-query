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

/* eslint-env mocha */
const assert = require('assert');
const sinon = require('sinon');
const path = require('path');
const { AssertionError } = require('assert');
const proxyquire = require('proxyquire');

const NodeHttpAdapter = require('@pollyjs/adapter-node-http');
const FSPersister = require('@pollyjs/persister-fs');
const { setupMocha: setupPolly } = require('@pollyjs/core');

// Register the node http adapter so its accessible by all future polly instances
const env = require('../src/env.js');

function getQuery(replacer) {
  return `--- Authorization: none
  SELECT req_url, count(req_http_X_CDN_Request_ID) AS visits, resp_http_Content_Type, status_code
  FROM ( 
    ^${replacer}
  )
  GROUP BY
    req_url, resp_http_Content_Type, status_code 
  ORDER BY visits DESC
  LIMIT @limit`;
}

describe('bigquery tests', async () => {
  const goodQuery = 'select req_url from requests LIMIT @limit';
  const badQuery = 'this query is intentionally broken.';

  const badExec = proxyquire('../src/sendquery.js', { './util.js': { loadQuery: () => badQuery } });
  const goodExec = proxyquire('../src/sendquery.js', { './util.js': { loadQuery: () => goodQuery } });
  const myReplacer = proxyquire('../src/sendquery.js', { './util.js': { loadQuery: () => getQuery('myrequests'), authFastly: () => true} });
  const execWithRealLoad = proxyquire('../src/sendquery.js', { './util.js': {authFastly: () => true}});

  const service = 'fake_name';

  setupPolly({
    recordFailedRequests: false,
    recordIfMissing: false,
    matchRequestsBy: {
      headers: {
        exclude: ['authorization', 'user-agent', 'x-goog-api-client'],
      },
      body: true,
      url: false,
      order: false,
      method: true,
    },
    logging: false,
    adapters: [NodeHttpAdapter],
    persister: FSPersister,
    persisterOptions: {
      fs: {
        recordingsDir: path.resolve(__dirname, 'fixtures/recordings'),
      },
    },
  });

  beforeEach(function first() {
    const { server } = this.polly;
    server.any(
      [
        'https://www.googleapis.com/oauth2/v4/token',
        'https://bigquery.googleapis.com/bigquery/v2/projects/helix-225321/jobs',
        'https://bigquery.googleapis.com/bigquery/v2/projects/helix-225321/datasets/helix_logging_fake_name',
      ],
    ).passthrough();

    server.any('https://bigquery.googleapis.com/bigquery/v2/projects/helix-225321/queries/*')
      .on('beforePersist', (req, recording) => {
        // eslint-disable-next-line no-param-reassign
        recording.request.headers['primary-key'] = 'Helix-Key';
      })
      .on('request', (req) => {
        req.headers['primary-key'] = 'Helix-Key';
      });
  });


  it('runs a query', async () => {
    const { results } = await goodExec.execute(env.email, env.key, env.projectid, 'list-everything', service, {
      limit: 3,
    });
    assert.ok(Array.isArray(results));
    assert.ok(results.length, 3);
  });

  it('runs a query with params', async () => {
    const { results } = await goodExec.execute(env.email, env.key, env.projectid, 'list-everything', service, {
      limit: 3,
    });
    assert.ok(Array.isArray(results));
    assert.equal(results.length, 3);
  });

  it('runs a query with myrequest replacer', async () => {
    const { results } = await myReplacer.execute(env.email, env.key, env.projectid, 'next-resource', service, {
      limit: 3,
    });
    assert.ok(Array.isArray(results));
    assert.equal(results.length, 3);
  });


  it('runs a query with alldatasets replacer', async () => {
    const { results } = await execWithRealLoad.execute(env.email, env.key, env.projectid, 'top-pages', service, {
      limit: 10,
      fromDays: 30,
      toDays: 0,
    });

    assert.ok(Array.isArray(results));
    assert.equal(results.length, 10);
  });


  it('throws without projectid', async () => {
    try {
      await goodExec.execute(env.email, env.key, undefined, 'list-everything', service);
      assert.fail('expected exception not thrown');
    } catch (e) {
      if (e instanceof AssertionError) {
        // bubble up the assertion error
        throw e;
      }
    }
  });

  it('throws with bad query', async () => {
    try {
      await badExec.execute(env.email, env.key, env.projectid, 'break-something', service);
      assert.fail('expected exception not thrown');
    } catch (e) {
      sinon.restore();
      if (e instanceof AssertionError) {
        // bubble up the assertion error
        throw e;
      }
    }
  });
});
