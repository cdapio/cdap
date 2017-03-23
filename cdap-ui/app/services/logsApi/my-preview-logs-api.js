/*
 * Copyright © 2017 Cask Data, Inc.
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
  .factory('myPreviewLogsApi', function(myCdapUrl, $resource, myAuth, myHelpers) {

    var url = myCdapUrl.constructUrl,
        basepath = '/namespaces/:namespace/previews/:previewId',
        logsPath = basepath + '/logs?';

    return $resource(
      url({ _cdapPath: basepath }),
    {
      namespace: '@namespace',
      previewId: '@previewId',
      start: '@start',
      stop: '@stop',
      fromOffset: '@fromOffset'
    },
    {
      getLogs: myHelpers.getConfig('GET', 'REQUEST', basepath + '/logs', true),
      getLogsStartAsJson: myHelpers.getConfig('GET', 'REQUEST', logsPath + 'format=json&max=100&start=:start', true),

      getLogsStartAsRaw: myHelpers.getConfig('GET', 'REQUEST', logsPath + 'start=:start', false, {
        interceptor: {
          // This is very lame. ngResource by default considers EVERYTHING as json and converts plain string to JSON
          // Thank you angular, $resource
          response: function(data) {
            return data.data;
          }
        }
      }),

      getLogsStatus: myHelpers.getConfig('GET', 'REQUEST', basepath + '/status', false),
      nextLogs: myHelpers.getConfig('GET', 'REQUEST', basepath + '/logs/next', true),
      nextLogsJson: myHelpers.getConfig('GET', 'REQUEST', basepath + '/logs/next?format=json', true),
      nextLogsJsonOffset: myHelpers.getConfig('GET', 'REQUEST', basepath + '/logs/next?format=json&max=100&fromOffset=:fromOffset', true),
      prevLogs: myHelpers.getConfig('GET', 'REQUEST', basepath + '/logs/prev', true),
      prevLogsJson: myHelpers.getConfig('GET', 'REQUEST', basepath + '/logs/prev?format=json', true),
    });
  });
