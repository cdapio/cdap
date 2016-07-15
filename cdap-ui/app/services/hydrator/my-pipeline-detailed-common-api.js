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
  .factory('myPipelineCommonApi', function($resource, myHelpers) {

    var basePath = '/namespaces/:namespace/apps/:app/:programType/:programName';
    var runsPath = basePath + '/runs';
    var schedulePath = '/namespaces/:namespace/apps/:app/schedules/:schedule';
    return $resource(
      '',
      {
        namespace: '@namespace',
        app: '@app',
        programType: '@programType',
        programName: '@programName',
        schedule: '@schedule'
      },
      {
        start: myHelpers.getConfig('POST', 'REQUEST', basePath + '/start'),
        stop: myHelpers.getConfig('POST', 'REQUEST', basePath + '/stop'),

        schedule: myHelpers.getConfig('POST', 'REQUEST', schedulePath + '/resume'),
        suspend: myHelpers.getConfig('POST', 'REQUEST', schedulePath + '/suspend'),
        scheduleStatus: myHelpers.getConfig('GET', 'REQUEST', schedulePath + '/status'),

        getStatistics: myHelpers.getConfig('GET', 'REQUEST', basePath + '/statistics'),
        getRuns: myHelpers.getConfig('GET', 'REQUEST', runsPath),
        pollRuns: myHelpers.getConfig('GET', 'POLL', basePath + '/runs', true),
        nextRunTime: myHelpers.getConfig('GET', 'REQUEST', basePath + '/nextruntime', true)
      }
    );
  });
