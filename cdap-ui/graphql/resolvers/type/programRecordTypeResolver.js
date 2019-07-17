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

var urlHelper = require('../../../server/url-helper'),
  cdapConfigurator = require('../../../cdap-config.js'),
  resolversCommon = require('../resolvers-common.js');

let cdapConfig;
cdapConfigurator.getCDAPConfig().then(function(value) {
  cdapConfig = value;
});

const runsResolver = async (programType, parent, args, context, info) => {
  const namespace = context.namespace;
  const name = parent.app;
  const programId = parent.name;
  const options = resolversCommon.getPOSTRequestOptions();
  options.headers.Authorization = context.auth;
  options['url'] = urlHelper.constructUrl(cdapConfig, `/v3/namespaces/${namespace}/runs`);
  options['body'] = [
    {
      appId: name,
      programType: programType,
      programId: programId,
    },
  ];

  const runInfo = await resolversCommon.requestPromiseWrapper(options);

  return runInfo[0].runs;
};

const programsTypeResolver = {
  ProgramRecord: {
    async __resolveType(parent, args, context, info) {
      return await new Promise((resolve, reject) => {
        switch (parent.type) {
          case 'Mapreduce':
            resolve('MapReduce');
            break;
          case 'Workflow':
            resolve('Workflow');
            break;
          case 'Spark':
            resolve('Spark');
            break;
          default:
            resolve(null);
        }
      });
    },
  },
  MapReduce: {
    runs: runsResolver.bind(null, 'mapreduce'),
  },
  Workflow: {
    runs: runsResolver.bind(null, 'workflow'),
  },
  Spark: {
    runs: runsResolver.bind(null, 'spark'),
  },
};

const programRecordTypeResolvers = programsTypeResolver;

module.exports = {
  programRecordTypeResolvers,
};
