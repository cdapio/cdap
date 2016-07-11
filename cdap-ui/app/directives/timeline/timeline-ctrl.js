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

function TimelineController ($scope, LogViewerStore, LOGVIEWERSTORE_ACTIONS, myLogsApi, myTimelineLogs, MyMetricsQueryHelper, MyCDAPDataSource) {

  var dataSrc = new MyCDAPDataSource($scope);
  $scope.metadata = [];
  //var loadTimeout = null;

  this.testLogs = {'name' : 'Patrick', 'age' : 29};
  console.log('From timeline controller: ', this.testLogs);

  this.testLogsFromService = myTimelineLogs.getLogs();

  this.updateStartTimeInStore = function(val) {
    LogViewerStore.dispatch({
      type: LOGVIEWERSTORE_ACTIONS.START_TIME,
      payload: {
        startTime: val
      }
    });
  };

  var namespace = 'default',
      appId = 'PurchaseHistory',
      programType = 'services',
      programId = 'PurchaseHistoryService',
      runId = '66747576-46fc-11e6-be39-56219b501a22',
      pollPromise = null;

  // var namespace = 'default',
  //     appId = 'HelloWorld',
  //     programType = 'flows',
  //     programId = 'WhoFlow',
  //     runId = '3d7bef02-453e-11e6-8c94-56219b501a22',
  //     pollPromise = null;

  // var metadataParams = {
  //   'namespace' : namespace,
  //   'appId' : appId,
  //   'programType' : programType,
  //   'programId' : programId,
  //   'runId' : runId
  // };


  var apiSettings = {
    metric : {
      context: 'namespace.' + namespace + '.app.' + appId + '.service.' + programId + '.run.' + runId ,
      names: ['system.app.log.error', 'system.app.log.warn', 'system.app.log.info'],
      startTime: 0,
      endTime : 'now',
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
      $scope.initialize();
      if (res.status === 'KILLED') {
        dataSrc.stopPoll(pollPromise.__pollId__);
        pollPromise = null;
        console.log('Status: KILLED - stopping polling..');
      }
    });
  }

  myLogsApi.getLogsMetadata({
    'namespace' : namespace,
    'appId' : appId,
    'programType' : programType,
    'programId' : programId,
    'runId' : runId,
  }).$promise.then(
    (res) => {
      console.log('Metadata in timeline : ' , res);

      apiSettings.metric.startTime = 1468196019;// = res.start;

      if(res.status==='KILLED'){
        apiSettings.metric.endTime = res.stop;
        console.log('Program is not running');
      } else if(res.status==='RUNNING'){
       // apiSettings.endTime = 'now';
        apiSettings.metric.endTime = 1468196079;
        console.log('Program is running');
        pollForMetadata();
      }
    },
    (err) => {
      console.log('ERROR: ', err);
    });
}

angular.module(PKG.name + '.commons')
.controller('TimelineController', TimelineController);
