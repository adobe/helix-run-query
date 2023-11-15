/*
 * Copyright 2023 Adobe. All rights reserved.
 * This file is licensed to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

import * as dotenv from 'dotenv';
import { DevelopmentServer } from '@adobe/helix-deploy';
import '../setup-env.js';
import { main } from '../../src/index.js';

dotenv.config();

function warnInvalidEnv() {
  const required = ['GOOGLE_CLIENT_EMAIL', 'GOOGLE_PRIVATE_KEY', 'GOOGLE_PROJECT_ID'];
  const missing = required.filter((req) => !process.env[req]);
  if (missing.length) {
    console.warn(`\n*WARNING* missing required env vars: ${missing.join(', ')}\n`);
  }
}

async function run() {
  warnInvalidEnv();
  const devServer = await new DevelopmentServer(main).init();
  await devServer.start();
}

run().then(process.stdout).catch(process.stderr);
