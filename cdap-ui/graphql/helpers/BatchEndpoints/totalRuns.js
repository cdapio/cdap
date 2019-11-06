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

const chunk = require('lodash/chunk');

let cdapConfig;
cdapConfigurator.getCDAPConfig().then(function(value) {
  cdapConfig = value;
});

async function batchTotalRuns(req, auth) {
  const namespace = req[0].namespace;
  const options = resolversCommon.getPOSTRequestOptions();
  options.url = urlHelper.constructUrl(cdapConfig, `/v3/namespaces/${namespace}/runcount`);
  const body = req.map((reqObj) => reqObj.program);
  const chunkedBody = chunk(body, 100);

  let runInfo = await Promise.all(
    chunkedBody.map((reqBody) => {
      const reqOptions = {
        ...options,
        body: reqBody,
      };
      return resolversCommon.requestPromiseWrapper(reqOptions, auth);
    })
  );

  const runsMap = {};
  runInfo.forEach((chunk) => {
    chunk.forEach((run) => {
      runsMap[run.appId] = run;
    });
  });

  return body.map((program) => {
    return runsMap[program.appId];
  });
}

module.exports = {
  batchTotalRuns,
};
