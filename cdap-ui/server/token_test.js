/*
 * Copyright © 2019 Cask Data, Inc.
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

var session = require('./token');
var assert = require('assert');

const fakeCDAPConfig = {
  'session.secret.key': 'secret-key-for-encryption',
};
function testMatch() {
  const token = session.generateToken(fakeCDAPConfig, console, 'Bearer 1111');
  const isTokenValid = session.validateToken(token, fakeCDAPConfig, console, 'Bearer 1111');
  console.log(`
    encrypted token: ${token}
    isToken valid: ${isTokenValid}
  `);
  assert(isTokenValid);
}
function testMismatch() {
  const token = session.generateToken(fakeCDAPConfig, console, 'Bearer 1111');
  const isTokenValid = session.validateToken(token, fakeCDAPConfig, console, 'Bearer 1112');
  console.log(`
    encrypted token: ${token}
    isToken valid: ${isTokenValid}
  `);
  assert(!isTokenValid);
}

testMatch();
testMismatch();
