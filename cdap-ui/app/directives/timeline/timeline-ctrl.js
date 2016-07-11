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
  $scope.metadata = [];

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
      startTime : '',//0,
      endTime : '',
      resolution: '1m'
    }
  };

  function pollForMetadata() {

    console.log('Before Poll: start: ', apiSettings.metric.startTime);
    console.log('end: ', apiSettings.metric.endTime);
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
      console.log('Polled Response: ', res);
      $scope.initialize();
      if (res.status === 'KILLED') {
        console.log('Status: KILLED - stopping polling..');
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
      //API returns seconds, not miliseconds
      let formattedStartTime = new Date(res.start *1000);
      formattedStartTime = ((formattedStartTime.getMonth() + 1) + '/' + formattedStartTime.getDate() + '/' + formattedStartTime.getFullYear() + ' ' + formattedStartTime.getHours() + ':' + formattedStartTime.getMinutes() + ':' + formattedStartTime.getSeconds());

      console.log('Start Time: ', formattedStartTime);

      //Set apiSettings - startTime to epoch (seconds)
      apiSettings.metric.startTime = res.start;
      if(res.status==='KILLED'){
        let formattedDate = new Date(res.stop*1000);
        formattedDate = ((formattedDate.getMonth() + 1) + '/' + formattedDate.getDate() + '/' + formattedDate.getFullYear() + ' ' + formattedDate.getHours() + ':' + formattedDate.getMinutes() + ':' + formattedDate.getSeconds());
        console.log('STATUS: KILLED - Stop time of Logs: ' + formattedDate);

        apiSettings.metric.endTime = res.stop;
      } else if(res.status==='RUNNING'){
        console.log('End time is now');
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
