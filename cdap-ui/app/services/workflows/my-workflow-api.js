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
  .factory('myWorkFlowApi', function($state, myCdapUrl, $resource, myHelpers) {

    var url = myCdapUrl.constructUrl,
        schedulepath = '/namespaces/:namespace/apps/:appId/schedules/:scheduleId',
        basepath = '/namespaces/:namespace/apps/:appId/workflows/:workflowId';


    return $resource(
      url({ _cdapPath: basepath }),
    {
      namespace: '@namespace',
      appId: '@appId',
      workflowId: '@workflowId',
      scheduleId: '@scheduleId',
      runId: '@runId'
    },
    {
      get: myHelpers.getConfig('GET', 'REQUEST', basepath),
      status: myHelpers.getConfig('GET', 'REQUEST', basepath + '/status'),
      start: myHelpers.getConfig('POST', 'REQUEST', basepath + '/start'),
      stop: myHelpers.getConfig('POST', 'REQUEST', basepath + '/stop'),
      pollStatus: myHelpers.getConfig('GET', 'POLL', basepath + '/status'),

      runs: myHelpers.getConfig('GET', 'REQUEST', basepath + '/runs', true),
      runDetail: myHelpers.getConfig('GET', 'REQUEST', basepath + '/runs/:runId'),
      pollRuns: myHelpers.getConfig('GET', 'POLL', basepath + '/runs', true),
      pollRunDetail: myHelpers.getConfig('GET', 'POLL', basepath + '/runs/:runId', false),
      pollRunDetailOften: myHelpers.getConfig('GET', 'POLL', basepath + '/runs/:runId', false, { interval: 1000 }),
      stopPollRunDetail: myHelpers.getConfig('GET', 'POLL-STOP', basepath + '/runs/:runId'),
      stopRun: myHelpers.getConfig('POST', 'REQUEST', basepath + '/runs/:runId/stop'),
      suspendRun: myHelpers.getConfig('POST', 'REQUEST', basepath + '/runs/:runId/suspend'),
      resumeRun: myHelpers.getConfig('POST', 'REQUEST', basepath + '/runs/:runId/resume'),

      nextLogs: myHelpers.getConfig('GET', 'REQUEST', basepath + '/runs/:runId/logs/next', true),
      prevLogs: myHelpers.getConfig('GET', 'REQUEST', basepath + '/runs/:runId/logs/prev', true),

      schedules: myHelpers.getConfig('GET', 'REQUEST', basepath + '/schedules', true),
      schedulesPreviousRunTime: myHelpers.getConfig('GET', 'REQUEST', basepath + '/previousruntime', true),
      pollScheduleStatus: myHelpers.getConfig('GET', 'POLL', schedulepath + '/status', false, { interval: 2000 }),
      getScheduleStatus: myHelpers.getConfig('GET', 'REQUEST', schedulepath + '/status'),
      scheduleSuspend: myHelpers.getConfig('POST', 'REQUEST', schedulepath + '/suspend'),
      scheduleResume: myHelpers.getConfig('POST', 'REQUEST', schedulepath + '/resume'),
      getCurrent: myHelpers.getConfig('GET', 'REQUEST', basepath + '/:runid/current', true),
      getUserNodeToken: myHelpers.getConfig('GET', 'REQUEST', basepath + '/runs/:runId/nodes/:nodeId/token?scope=user'),
      getSystemNodeToken: myHelpers.getConfig('GET', 'REQUEST', basepath + '/runs/:runId/nodes/:nodeId/token?scope=system'),
      getStatistics: myHelpers.getConfig('GET', 'REQUEST', basepath + '/statistics'),
      doAction: myHelpers.getConfig('POST', 'REQUEST', basepath + '/:action')
    });
  });
