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

describe('Test Queries', () => {
  it('rum-dashboard', async () => {
    const res = await main(new Request('https://helix-run-query.com/rum-dashboard?url=www.hlx.live&domain=www.hlx.live', {
      headers: {
        Authorization: `Bearer ${process.env.UNIVERSAL_TOKEN}`,
      },
    }), {
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
    assert.ok(json.meta.data.filter((e) => e.name === 'domainkey').length === 0, 'domainkey should not be in requestParams');
  }).timeout(100000);

  it('rum-dashboard (url auth)', async () => {
    const res = await main(new Request(`https://helix-run-query.com/rum-dashboard?url=www.hlx.live&domain=www.hlx.live&domainkey=${process.env.UNIVERSAL_TOKEN}`, {
    }), {
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
    assert.ok(json.meta.data.filter((e) => e.name === 'domainkey').length === 0, 'domainkey should not be in requestParams');
  }).timeout(100000);

  it('rotate-domainkeys (success)', async () => {
    const res = await main(new Request('https://helix-run-query.com/rotate-domainkeys?url=test.adobe.com', {
      headers: {
        Authorization: `Bearer ${process.env.UNIVERSAL_TOKEN}`,
      },
    }), {
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
    assert.ok(json.meta.data.filter((e) => e.name === 'domainkey').length === 0, 'domainkey should not be in requestParams');
  }).timeout(100000);

  it('rotate-domainkeys (force)', async () => {
    const res = await main(new Request('https://helix-run-query.com/rotate-domainkeys', {
      headers: {
        Authorization: `Bearer ${process.env.UNIVERSAL_TOKEN}`,
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      method: 'POST',
      body: `url=test.adobe.com&newkey=${Math.random()}key`,
    }), {
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
    assert.ok(json.meta.data.filter((e) => e.name === 'domainkey').length === 0, 'domainkey should not be in requestParams');
  }).timeout(100000);

  it('rotate-domainkeys (failure)', async () => {
    const res = await main(new Request('https://helix-run-query.com/rotate-domainkeys?url=test.adobe.com', {
      headers: {
        Authorization: 'Bearer Invalidsecret',
      },
    }), {
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
    assert.ok(json.meta.data.filter((e) => e.name === 'domainkey').length === 0, 'domainkey should not be in requestParams');
  }).timeout(100000);
});
