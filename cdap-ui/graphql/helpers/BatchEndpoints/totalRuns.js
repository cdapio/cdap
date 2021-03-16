/*
 * Copyright © 2019-2020 Cask Data, Inc.
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

import { constructUrl } from 'server/url-helper';
import { getCDAPConfig } from 'server/cdap-config';
import { getPOSTRequestOptions, requestPromiseWrapper } from 'gql/resolvers-common';
import { ApolloError } from 'apollo-server';
import chunk from 'lodash/chunk';

let cdapConfig;
getCDAPConfig().then(function(value) {
  cdapConfig = value;
});

export async function batchTotalRuns(req, auth, userIdProperty, userIdValue) {
  const namespace = req[0].namespace;
  const options = getPOSTRequestOptions();
  options.url = constructUrl(cdapConfig, `/v3/namespaces/${namespace}/runcount`);

  const errorModifiersFn = (error, statusCode) => {
    return new ApolloError(error, statusCode, { errorOrigin: 'totalRuns' });
  }

  const body = req.slice(0, 25).map((reqObj) => reqObj.program);
  const chunkedBody = chunk(body, 100);

  let runInfo = await Promise.all(
    chunkedBody.map((reqBody) => {
      const reqOptions = {
        ...options,
        body: reqBody,
      };
      const authContext = {
        auth,
        userIdProperty,
        userIdValue
      };
      return requestPromiseWrapper(reqOptions, authContext, null, errorModifiersFn);
    })
  );

  const runsMap = {};
  runInfo.forEach((chunk) => {
    chunk.forEach((run) => {
      runsMap[run.appId] = run;
    });
  });

  return req.map(({ program }) => {
    return runsMap[program.appId];
  });
}
