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

function TimelineController ($scope, LogViewerStore, LOGVIEWERSTORE_ACTIONS, myLogsApi, MyMetricsQueryHelper, MyCDAPDataSource, ProgramsHelpers, moment, $timeout, caskWindowManager) {

  var dataSrc = new MyCDAPDataSource($scope);
  $scope.moment = moment;
  let screenSize;
  $scope.globalStartTime = null;

  //Default values for scrollPosition
  this.pinScrollPosition = 0;
  $scope.pinScrollingPosition = 0;

  this.updateStartTimeInStore = function(val) {
    LogViewerStore.dispatch({
      type: LOGVIEWERSTORE_ACTIONS.START_TIME,
      payload: {
        startTime: val
      }
    });
  };

  //Re-render timeline when screen resizes
  $scope.$on(caskWindowManager.event.resize, () => {
    $timeout($scope.initialize);
  });

  this.updateTotalLogsInStore = function(val) {
    LogViewerStore.dispatch({
      type: LOGVIEWERSTORE_ACTIONS.TOTAL_LOGS,
      payload: {
        totalLogs: val
      }
    });
  };
  this.updateScrollPositionInStore = function(val) {
    LogViewerStore.dispatch({
      type: LOGVIEWERSTORE_ACTIONS.SCROLL_POSITION,
      payload: {
        scrollPosition: val
      }
    });
  };

  this.updateTotalErrorsInStore = function(val) {
    LogViewerStore.dispatch({
      type: LOGVIEWERSTORE_ACTIONS.TOTAL_ERRORS,
      payload: {
        totalErrors: val
      }
    });
  };

  this.updateTotalWarningsInStore = function(val) {
    LogViewerStore.dispatch({
      type: LOGVIEWERSTORE_ACTIONS.TOTAL_WARNINGS,
      payload: {
        totalWarnings: val
      }
    });
  };

  var pollPromise = null;
  var programType = ProgramsHelpers.getSingularName(this.programType);

  var apiSettings = {
    metric : {
      context: `namespace.${this.namespaceId}.app.${this.appId}.${programType}.${this.programId}.run.${this.runId}`,
      names: ['system.app.log.error', 'system.app.log.warn', 'system.app.log.info', 'system.app.log.debug'],
      startTime : '',
      endTime : '',
      resolution: '1m'
    }
  };

  this.setDefaultTimeWindow = () => {
    apiSettings.metric.startTime = $scope.globalStartTime;
    apiSettings.metric.endTime = '';
    this.updateStartTimeInStore($scope.globalStartTime);
  };

  const pollForMetadata = () => {
    pollPromise = dataSrc.poll({
      _cdapPath: '/metrics/query',
      method: 'POST',
      body: MyMetricsQueryHelper.constructQuery(
        'qid',
        MyMetricsQueryHelper.contextToTags(apiSettings.metric.context),
        apiSettings.metric
      )
    },
    (res) => {
      $scope.metadata = res;
      $scope.sliderBarPositionRefresh = LogViewerStore.getState().startTime;
      $scope.initialize();
      if (res.status === 'KILLED' || res.status==='COMPLETED' || res.status === 'FAILED' || res.status === 'STOPPED') {
        dataSrc.stopPoll(pollPromise.__pollId__);
        pollPromise = null;
      }
    }, (err) => {
      // FIXME: We need to fix this. Right now this fails and we need to handle this more gracefully.
      $scope.initialize();
      console.log('ERROR: ', err);
    });
  };

  LogViewerStore.subscribe(() => {

    //'*1000' converts from seconds to miliseconds
    if($scope.globalStartTime === null || $scope.globalStartTime !== LogViewerStore.getState().globalStartTime * 1000){
      $scope.globalStartTime = LogViewerStore.getState().globalStartTime * 1000;
      return;
    }

    if(screenSize !== LogViewerStore.getState().fullScreen){
      screenSize = LogViewerStore.getState().fullScreen;
      $timeout($scope.initialize);
    }

    //Keep the slider handle in sync with the api call
    if(typeof $scope.updateSliderHandle !== 'undefined'){
      $scope.updateSliderHandle(LogViewerStore.getState().startTime);

      //Check if the pinScrollPosition is less than the value of the query handle
      this.pinScrollPosition = LogViewerStore.getState().scrollPosition;
      if(typeof $scope.updatePin !== 'undefined'){
        $scope.pinScrollingPosition = this.pinScrollPosition;
        $scope.updatePin();
      }

      if($scope.searchResultTimes !== LogViewerStore.getState().searchResults){
        $scope.searchResultTimes = LogViewerStore.getState().searchResults;
        $scope.renderSearchCircles($scope.searchResultTimes);
      }
    }

  });

  screenSize = LogViewerStore.getState().fullScreen;

  if (!this.namespaceId || !this.appId || !this.programType || !this.programId || !this.runId) {
    this.setDefaultTimeWindow();
    return;
  }

  myLogsApi.getLogsMetadata({
    namespace : this.namespaceId,
    appId : this.appId,
    programType : this.programType,
    programId : this.programId,
    runId : this.runId,
  }).$promise.then(
    (res) => {
      $scope.metadata = res;
      if(res.start === res.end){
        res.end++;
      }
      apiSettings.metric.startTime = res.start;
      apiSettings.metric.endTime = res.end;
      $scope.renderSearchCircles([]);
      pollForMetadata();
    },
    (err) => {
      this.setDefaultTimeWindow();
      console.log('ERROR: ', err);
    });
}

angular.module(PKG.name + '.commons')
.controller('TimelineController', TimelineController);
