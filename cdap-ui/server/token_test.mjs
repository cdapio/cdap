/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

// Execute this file with "node --experimental-modules"

import { generateToken, validateToken } from './token.mjs';
import assert from 'assert';

const fakeCDAPConfig = {
  'session.secret.key': 'secret-key-for-encryption',
};
function testMatch() {
  const token = generateToken(fakeCDAPConfig, console, 'Bearer 1111');
  const isTokenValid = validateToken(token, fakeCDAPConfig, console, 'Bearer 1111');
  console.log(`
    encrypted token: ${token}
    isToken valid: ${isTokenValid}
  `);
  assert(isTokenValid);
}
function testMismatch() {
  const token = generateToken(fakeCDAPConfig, console, 'Bearer 1111');
  const isTokenValid = validateToken(token, fakeCDAPConfig, console, 'Bearer 1112');
  console.log(`
    encrypted token: ${token}
    isToken valid: ${isTokenValid}
  `);
  assert(!isTokenValid);
}

testMatch();
testMismatch();
