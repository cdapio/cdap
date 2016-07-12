/*
 * Copyright © 2016 Cask Data, Inc.
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

function TimelineController ($scope, LogViewerStore, LOGVIEWERSTORE_ACTIONS, myLogsApi, MyMetricsQueryHelper, MyCDAPDataSource) {

  var dataSrc = new MyCDAPDataSource($scope);

  this.updateStartTimeInStore = function(val) {
    LogViewerStore.dispatch({
      type: LOGVIEWERSTORE_ACTIONS.START_TIME,
      payload: {
        startTime: val
      }
    });
  };

  var pollPromise = null;

  var apiSettings = {
    metric : {
      context: `namespace.${this.namespaceId}.app.${this.appId}.flow.${this.programId}.run.${this.runId}`,
      names: ['system.app.log.error', 'system.app.log.warn', 'system.app.log.info', 'system.app.log.debug'],
      startTime : '',
      endTime : '',
      resolution: '1m'
    }
  };

  function pollForMetadata() {
    pollPromise = dataSrc.poll({
      _cdapPath: '/metrics/query',
      method: 'POST',
      body: MyMetricsQueryHelper.constructQuery(
        'qid',
        MyMetricsQueryHelper.contextToTags(apiSettings.metric.context),
        apiSettings.metric
      )
    },
    function (res) {
      $scope.metadata = res;
      $scope.sliderBarPositionRefresh = LogViewerStore.getState().startTime;
      $scope.initialize();
      if (res.status === 'KILLED') {
        dataSrc.stopPoll(pollPromise.__pollId__);
        pollPromise = null;
      }
    }, function(err) {
      console.log('ERROR: ', err);
    });
  }

  myLogsApi.getLogsMetadata({
    'namespace' : this.namespaceId,
    'appId' : this.appId,
    'programType' : this.programType,
    'programId' : this.programId,
    'runId' : this.runId,
  }).$promise.then(
    (res) => {
      apiSettings.metric.startTime = res.start;
      if (res.status==='KILLED') {
        apiSettings.metric.endTime = 'now';
        pollForMetadata();
      } else if (res.status==='RUNNING') {
        apiSettings.metric.endTime = 'now';
        pollForMetadata();
      }
    },
    (err) => {
      console.log('ERROR: ', err);
    });
}

angular.module(PKG.name + '.commons')
.controller('TimelineController', TimelineController);
