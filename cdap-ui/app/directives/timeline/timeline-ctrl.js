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

function TimelineController ($scope, LogViewerStore, LOGVIEWERSTORE_ACTIONS, myLogsApi, myTimelineLogs, MyCDAPDataSource) {

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

  // var namespace = 'default',
  //     appId = 'HelloWorld',
  //     programType = 'flows',
  //     programId = 'WhoFlow',
  //     runId = '3d7bef02-453e-11e6-8c94-56219b501a22',
  //     pollPromise = null;

  var namespace = 'default',
      appId = 'PurchaseHistory',
      programType = 'flows',
      programId = 'PurchaseFlow',
      runId = '53522903-46e4-11e6-b2ff-56219b501a22',
      pollPromise = null;

  var metadataParams = {
    'namespace' : namespace,
    'appId' : appId,
    'programType' : programType,
    'programId' : programId,
    'runId' : runId
  };

  function pollForMetadata(params) {
    var path = '/namespaces/' + params.namespace +
      '/apps/' + params.appId +
      '/' + params.programType + '/' + params.programId +
      '/runs/' + params.runId;

    pollPromise = dataSrc.poll({
      _cdapPath: path,
      interval: 3000
    }, function (res) {
      $scope.metadata = res;

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

      if(res.status==='KILLED'){
        console.log('Program is not running');
      } else if(res.status==='RUNNING'){
        //Poll the API for data every 10 seconds
        console.log('Program is running');
        pollForMetadata(metadataParams);
      }
    },
    (err) => {
      console.log('ERROR: ', err);
    });
  // myLogsApi.nextLogs({
  //   'namespace' : namespace,
  //   'appId' : appId,
  //   'programType' : programType,
  //   'programId' : programId,
  //   'runId' : runId,
  // }).$promise.then(
  //   (res) => {
  //     //Process the data
  //     angular.forEach(res, (element, index) => {
  //       let formattedDate = new Date(res[index].log.timestamp);
  //       res[index].log.timestamp = formattedDate;
  //     });
  //     this.timelineData = res;
  //     console.log('data fetched from timeline controller');
  //   },
  //   (err) => {
  //     console.log('ERROR: ', err);
  //   });
}

angular.module(PKG.name + '.commons')
.controller('TimelineController', TimelineController);
