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

const urlHelper = require('../../../server/url-helper'),
  cdapConfigurator = require('../../../server/cdap-config.js'),
  resolversCommon = require('../../resolvers-common.js');

let cdapConfig;
cdapConfigurator.getCDAPConfig().then(function(value) {
  cdapConfig = value;
});

async function batchProgramRuns(req, auth) {
  const namespace = req[0].namespace;
  const options = resolversCommon.getPOSTRequestOptions();
  options.url = urlHelper.constructUrl(cdapConfig, `/v3/namespaces/${namespace}/runs`);
  options.body = req.slice(0, 25).map((reqObj) => reqObj.program);

  const runInfo = await resolversCommon.requestPromiseWrapper(options, auth);

  const runsMap = {};
  runInfo.forEach((run) => {
    runsMap[run.appId] = run;
  });

  // DataLoader requires the response to be in the same order as the request. However, the backend
  // do not guarantee this, therefore we are creating a map for lookup to maintain the order.
  return req.map(({ program }) => {
    return runsMap[program.appId];
  });
}

module.exports = {
  batchProgramRuns,
};
