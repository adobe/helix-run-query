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

import assert from 'assert';
import { Request } from '@adobe/fetch';

import { main } from '../src/index.js';

describe('Test ContentRequests', () => {
  it('rum-contentrequests should provide monthly data', async () => {
    const res = await main(new Request('https://helix-run-query.com/rum-contentrequests'
      + '?url=dxc.com'
      + '&granularity=30'
      + '&offset=-1'
      + '&interval=-1'
      + '&startdate=2024-03-30'
      + '&enddate=2024-04-01'
      + '&timezone=UTC'
      + `&domainkey=${process.env.UNIVERSAL_TOKEN}`, {}), {
      env: {
        GOOGLE_CLIENT_EMAIL: process.env.GOOGLE_CLIENT_EMAIL,
        GOOGLE_PRIVATE_KEY: process.env.GOOGLE_PRIVATE_KEY,
        GOOGLE_PROJECT_ID: process.env.GOOGLE_PROJECT_ID,
      },
    });
    assert.ok(res);
    const results = await res.text();
    assert.ok(results);
    let json;
    try {
      json = JSON.parse(results);
    } catch (e) {
      assert.fail(`${results} is not valid JSON`);
    }
    assert.ok(json.results.data);
    assert.ok(json.results.data.length === 2, 'expecting 2 entries, 1 for march 2024, 1 for april 2024');
    const expected = [
      {
        id: 'dxc.com-1709251200000000',
        year: 2024,
        month: 3,
        day: 1,
        hostname: 'dxc.com',
        content_requests: 25160,
        content_requests_marginal_err_excl: 22051,
        content_requests_marginal_err_incl: 28269,
        pageviews: 24400,
        apicalls: 3800,
        html_requests: 28400,
        json_requests: 4300,
        error404_requests: 4000,
        rownum: 1,
        time: '2024-03-01T00:00:00+00:00',
      },
      {
        id: 'dxc.com-1711929600000000',
        year: 2024,
        month: 4,
        day: 1,
        hostname: 'dxc.com',
        content_requests: 18620,
        content_requests_marginal_err_excl: 15945,
        content_requests_marginal_err_incl: 21295,
        pageviews: 18100,
        apicalls: 2600,
        html_requests: 19900,
        json_requests: 2900,
        error404_requests: 1800,
        rownum: 2,
        time: '2024-04-01T00:00:00+00:00',
      },
    ];
    const actual = json.results.data;
    assert.deepEqual(actual, expected);
    assert.ok(json.meta.data.filter((e) => e.name === 'domainkey').length === 0, 'domainkey should not be in requestParams');
  }).timeout(100000);

  it('rum-contentrequests should provide yearly data', async () => {
    const res = await main(new Request('https://helix-run-query.com/rum-contentrequests'
      + '?url=dxc.com'
      + '&granularity=365'
      + '&offset=-1'
      + '&interval=-1'
      + '&startdate=2024-03-30'
      + '&enddate=2024-04-01'
      + '&timezone=UTC'
      + `&domainkey=${process.env.UNIVERSAL_TOKEN}`, {}), {
      env: {
        GOOGLE_CLIENT_EMAIL: process.env.GOOGLE_CLIENT_EMAIL,
        GOOGLE_PRIVATE_KEY: process.env.GOOGLE_PRIVATE_KEY,
        GOOGLE_PROJECT_ID: process.env.GOOGLE_PROJECT_ID,
      },
    });
    assert.ok(res);
    const results = await res.text();
    assert.ok(results);
    let json;
    try {
      json = JSON.parse(results);
    } catch (e) {
      assert.fail(`${results} is not valid JSON`);
    }
    assert.ok(json.results.data);
    assert.ok(json.results.data.length === 1, 'expecting 1 entries, 1 for year 2024');
    const expected = [
      {
        id: 'dxc.com-1704067200000000',
        year: 2024,
        month: 1,
        day: 1,
        hostname: 'dxc.com',
        content_requests: 43780,
        content_requests_marginal_err_excl: 39679,
        content_requests_marginal_err_incl: 47881,
        pageviews: 42500,
        apicalls: 6400,
        html_requests: 48300,
        json_requests: 7200,
        error404_requests: 5800,
        rownum: 1,
        time: '2024-01-01T00:00:00+00:00',
      },
    ];
    const actual = json.results.data;
    assert.deepEqual(actual, expected);
    assert.ok(json.meta.data.filter((e) => e.name === 'domainkey').length === 0, 'domainkey should not be in requestParams');
  }).timeout(100000);

  it('rum-contentrequests should provide data for multiple hosts', async () => {
    const res = await main(new Request('https://helix-run-query.com/rum-contentrequests'
      + '?url=dxc.com,blog.adobe.com'
      + '&granularity=365'
      + '&offset=-1'
      + '&interval=-1'
      + '&startdate=2024-03-30'
      + '&enddate=2024-04-01'
      + '&timezone=UTC'
      + `&domainkey=${process.env.UNIVERSAL_TOKEN}`, {}), {
      env: {
        GOOGLE_CLIENT_EMAIL: process.env.GOOGLE_CLIENT_EMAIL,
        GOOGLE_PRIVATE_KEY: process.env.GOOGLE_PRIVATE_KEY,
        GOOGLE_PROJECT_ID: process.env.GOOGLE_PROJECT_ID,
      },
    });
    assert.ok(res);
    const results = await res.text();
    assert.ok(results);
    let json;
    try {
      json = JSON.parse(results);
    } catch (e) {
      assert.fail(`${results} is not valid JSON`);
    }
    assert.ok(json.results.data);
    assert.ok(json.results.data.length === 2, 'expecting 2 entries, 1 for each host for year 2024');
    const expected = [
      {
        id: 'blog.adobe.com-1704067200000000',
        year: 2024,
        month: 1,
        day: 1,
        hostname: 'blog.adobe.com',
        content_requests: 225770,
        content_requests_marginal_err_excl: 216457,
        content_requests_marginal_err_incl: 235083,
        pageviews: 107130,
        apicalls: 593200,
        html_requests: 113030,
        json_requests: 623200,
        error404_requests: 3200,
        rownum: 1,
        time: '2024-01-01T00:00:00+00:00',
      },
      {
        id: 'dxc.com-1704067200000000',
        year: 2024,
        month: 1,
        day: 1,
        hostname: 'dxc.com',
        content_requests: 43780,
        content_requests_marginal_err_excl: 39679,
        content_requests_marginal_err_incl: 47881,
        pageviews: 42500,
        apicalls: 6400,
        html_requests: 48300,
        json_requests: 7200,
        error404_requests: 5800,
        rownum: 1,
        time: '2024-01-01T00:00:00+00:00',
      },
    ];
    const actual = json.results.data;
    assert.deepEqual(actual, expected);
    assert.ok(json.meta.data.filter((e) => e.name === 'domainkey').length === 0, 'domainkey should not be in requestParams');
  }).timeout(100000);

  it('rum-contentrequests should provide the second page of the monthly data', async () => {
    const res = await main(new Request('https://helix-run-query.com/rum-contentrequests'
      + '?url=dxc.com'
      + '&granularity=30'
      + '&offset=-1'
      + '&interval=-1'
      + '&startdate=2024-03-30'
      + '&enddate=2024-04-01'
      + '&timezone=UTC'
      + '&limit=1'
      + '&after=dxc.com-1709251200000000'
      + `&domainkey=${process.env.UNIVERSAL_TOKEN}`, {}), {
      env: {
        GOOGLE_CLIENT_EMAIL: process.env.GOOGLE_CLIENT_EMAIL,
        GOOGLE_PRIVATE_KEY: process.env.GOOGLE_PRIVATE_KEY,
        GOOGLE_PROJECT_ID: process.env.GOOGLE_PROJECT_ID,
      },
    });
    assert.ok(res);
    const results = await res.text();
    assert.ok(results);
    let json;
    try {
      json = JSON.parse(results);
    } catch (e) {
      assert.fail(`${results} is not valid JSON`);
    }
    assert.ok(json.results.data);
    assert.ok(json.results.data.length === 1, 'expecting 1 entry for april 2024');
    const expected = [
      {
        id: 'dxc.com-1711929600000000',
        year: 2024,
        month: 4,
        day: 1,
        hostname: 'dxc.com',
        content_requests: 18620,
        content_requests_marginal_err_excl: 15945,
        content_requests_marginal_err_incl: 21295,
        pageviews: 18100,
        apicalls: 2600,
        html_requests: 19900,
        json_requests: 2900,
        error404_requests: 1800,
        rownum: 2,
        time: '2024-04-01T00:00:00+00:00',
      },
    ];
    const actual = json.results.data;
    assert.deepEqual(actual, expected);
    assert.ok(json.meta.data.filter((e) => e.name === 'domainkey').length === 0, 'domainkey should not be in requestParams');
  }).timeout(100000);
});
