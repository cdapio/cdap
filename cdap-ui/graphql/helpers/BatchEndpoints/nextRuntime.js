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

const { constructUrl } = require('../../../server/url-helper'),
  { getCDAPConfig } = require('../../../server/cdap-config.js'),
  { getPOSTRequestOptions, requestPromiseWrapper } = require('../../resolvers-common.js');

let cdapConfig;
getCDAPConfig().then(function(value) {
  cdapConfig = value;
});

async function batchNextRuntime(req, auth) {
  const namespace = req[0].namespace;
  const options = getPOSTRequestOptions();
  options.url = constructUrl(cdapConfig, `/v3/namespaces/${namespace}/nextruntime`);
  options.errorOrigin ='nextRuntime';
  const body = req.slice(0, 25).map((reqObj) => reqObj.program);
  options.body = body;

  let nextRuntimeInfo = await requestPromiseWrapper(options, auth);

  const runsMap = {};
  nextRuntimeInfo.forEach((run) => {
    if (run.schedules) {
      runsMap[run.appId] = run.schedules;
    } else {
      runsMap[run.appId] = [];
    }
  });

  return req.map(({ program }) => {
    return runsMap[program.appId];
  });
}

module.exports = {
  batchNextRuntime,
};
