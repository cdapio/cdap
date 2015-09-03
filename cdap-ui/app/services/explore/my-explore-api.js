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
  .factory('myExploreApi', function(myCdapUrl, $resource, myAuth, myHelpers) {

    var url = myCdapUrl.constructUrl,
        basepath = '/namespaces/:namespace/data/explore/tables',
        querypathNs = '/namespaces/:namespace/data/explore/queries',
        querypath = '/data/explore/queries/:queryhandle';

    return $resource(
      url({ _cdapPath: basepath }),
    {
      table: '@table',
      queryhandle: '@queryhandle'
    },
    {
      list: myHelpers.getConfig('GET', 'REQUEST', basepath, true),
      getInfo: myHelpers.getConfig('GET', 'REQUEST', basepath + '/:table/info'),
      postQuery: myHelpers.getConfig('POST', 'REQUEST', querypathNs),
      getQueries: myHelpers.getConfig('GET', 'REQUEST', querypathNs, true),
      getQuerySchema: myHelpers.getConfig('GET', 'REQUEST', querypath + '/schema', true),
      getQueryPreview: myHelpers.getConfig('POST', 'REQUEST', querypath + '/preview', true),
      pollQueryStatus: myHelpers.getConfig('GET', 'POLL', querypath + '/status', false, { interval: 2000 }),
      stopPollQueryStatus: myHelpers.getConfig('GET', 'POLL-STOP', querypath + '/status')
    });
  });
