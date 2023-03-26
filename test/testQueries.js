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
