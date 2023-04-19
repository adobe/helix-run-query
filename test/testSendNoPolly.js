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
import assert from 'assert';
import esmock from 'esmock';
// Register the node http adapter so its accessible by all future polly instances
import env from '../src/env.js';

describe('bigquery tests (online)', async () => {
  const service = undefined;

  it.skip('runs a query with alldatasets replacer', async () => {
    const execWithRealLoad = await esmock('../src/sendquery.js', { '../src/util.js': { authFastly: () => true } });
    const { results, description, requestParams } = await execWithRealLoad.execute(env.email, env.key, env.projectid, 'top-pages', service, {
      limit: 10,
      fromDays: 30,
      toDays: 0,
    });

    assert.ok(Array.isArray(results));
    assert.equal(description, 'most requested sites by Helix.');
    assert.deepEqual(requestParams, { limit: 10, fromDays: 30, toDays: 0 });
    assert.equal(results.length, 10);
  }).timeout(15000);
});
