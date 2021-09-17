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

/* eslint-env mocha */

const assert = require('assert');
const { Request } = require('@adobe/helix-universal');
const { main } = require('../src/index');

describe('Test Queries', () => {
  it('recent-errors', async () => {
    const res = await main(new Request('https://helix-run-query.com/recent-errors', {
      headers: {
        'x-token': process.env.HLX_FASTLY_AUTH,
        'x-service': '6v0sHgrPTGUGS5PHOXZ0H1',
      },
    }), {
      env: {
        GOOGLE_CLIENT_EMAIL: process.env.GOOGLE_CLIENT_EMAIL,
        GOOGLE_PRIVATE_KEY: process.env.GOOGLE_PRIVATE_KEY,
        GOOGLE_PROJECT_ID: process.env.GOOGLE_PROJECT_ID,
      },
    });
    assert.ok(res);
    const results = await res.json();
    assert.ok(results);
    console.table(results.results);
  }).timeout(100000);

  it('next-resource', async () => {
    const res = await main(new Request('https://helix-run-query.com/next-resource', {
      headers: {
        'x-token': process.env.HLX_FASTLY_AUTH,
        'x-service': '6v0sHgrPTGUGS5PHOXZ0H1',
      },
    }), {
      env: {
        GOOGLE_CLIENT_EMAIL: process.env.GOOGLE_CLIENT_EMAIL,
        GOOGLE_PRIVATE_KEY: process.env.GOOGLE_PRIVATE_KEY,
        GOOGLE_PROJECT_ID: process.env.GOOGLE_PROJECT_ID,
      },
    });
    assert.ok(res);
    const results = await res.json();
    assert.ok(results);
    console.table(results.results);
  }).timeout(100000);

  it('most-visited', async () => {
    const res = await main(new Request('https://helix-run-query.com/most-visited', {
      headers: {
        'x-token': process.env.HLX_FASTLY_AUTH,
        'x-service': '6v0sHgrPTGUGS5PHOXZ0H1',
      },
    }), {
      env: {
        GOOGLE_CLIENT_EMAIL: process.env.GOOGLE_CLIENT_EMAIL,
        GOOGLE_PRIVATE_KEY: process.env.GOOGLE_PRIVATE_KEY,
        GOOGLE_PROJECT_ID: process.env.GOOGLE_PROJECT_ID,
      },
    });
    assert.ok(res);
    const results = await res.json();
    assert.ok(results);
    console.table(results.results);
  }).timeout(100000);
});

it('pageviews-by-generation', async () => {
  const res = await main(new Request('https://helix-run-query.com/pageviews-by-generation?domain=blog.adobe.com&generations=instrumentation-test', {
    headers: {
    },
  }), {
    env: {
      GOOGLE_CLIENT_EMAIL: process.env.GOOGLE_CLIENT_EMAIL,
      GOOGLE_PRIVATE_KEY: process.env.GOOGLE_PRIVATE_KEY,
      GOOGLE_PROJECT_ID: process.env.GOOGLE_PROJECT_ID,
    },
  });
  assert.ok(res);
  const results = await res.json();
  assert.ok(results);
  console.table(results.results);
}).timeout(100000);
