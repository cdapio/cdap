/*
 * Copyright © 2015 Cask Data, Inc.
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

angular.module(PKG.name + '.services')
  .factory('myWorkersApi', function(myCdapUrl, $resource, myAuth, myHelpers) {

    var url = myCdapUrl.constructUrl,
        basepath = '/namespaces/:namespace/apps/:appId/workers/:workerId';

    return $resource(
      url({ _cdapPath: basepath }),
    {
      namespace: '@namespace',
      appId: '@appId',
      sparkId: '@mapreduceId',
      runId: '@runId'
    },
    {
      get: myHelpers.getConfig('GET', 'REQUEST', basepath),
      runs: myHelpers.getConfig('GET', 'REQUEST', basepath + '/runs', true),
      nextLogs: myHelpers.getConfig('GET', 'REQUEST', basepath + '/runs/:runId/logs/next', true),
      prevLogs: myHelpers.getConfig('GET', 'REQUEST', basepath + '/runs/:runId/logs/prev', true),
      runDetail: myHelpers.getConfig('GET', 'REQUEST', basepath + '/runs/:runId'),
      doAction: myHelpers.getConfig('POST', 'REQUEST', basepath + '/:action'),
      getProgramStatus: myHelpers.getConfig('GET', 'REQUEST', basepath + '/status'),
      pollStatus: myHelpers.getConfig('GET', 'POLL', basepath + '/status')
    });
  });
