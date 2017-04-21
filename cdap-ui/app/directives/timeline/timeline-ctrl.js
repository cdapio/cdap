/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

  this.dataSrc = new MyCDAPDataSource($scope);
  this.pinScrollPosition = 0;
  this.screenSize = LogViewerStore.getState().fullScreen;
  this.pollPromise = null;

  let programTypeSingular = ProgramsHelpers.getSingularName(this.programType);
  this.apiSettings = {
    metric : {
      context: `namespace.${this.namespaceId}.app.${this.appId}.${programTypeSingular}.${this.programId}.run.${this.runId}`,
      names: ['system.app.log.error', 'system.app.log.warn', 'system.app.log.info', 'system.app.log.debug', 'system.app.log.trace'],
      startTime : '',
      endTime : '',
      resolution: 'auto'
    }
  };

  $scope.moment = moment;
  $scope.pinScrollingPosition = 0;

  $scope.$on(caskWindowManager.event.resize, () => {
    $timeout($scope.initialize);
  });

  this.updateStartTimeInStore = function(val) {
    LogViewerStore.dispatch({
      type: LOGVIEWERSTORE_ACTIONS.START_TIME,
      payload: {
        startTime: val
      }
    });
  };

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

  this.setDefaultTimeWindow = () => {
    this.apiSettings.metric.startTime = '';
    this.apiSettings.metric.endTime = '';
    this.updateStartTimeInStore(this.apiSettings.metric.startTime);
  };

  this.getStatus = () => {
    if (!this.pollPromise) {
      return;
    }
    myLogsApi.getLogsMetadata({
      namespace : this.namespaceId,
      appId : this.appId,
      programType : this.programType,
      programId : this.programId,
      runId : this.runId
    }).$promise.then(
      (res) => {
        let runStatuses =  ['KILLED', 'COMPLETED', 'FAILED', 'RUN_FAILED', 'STOPPED', 'KILLED_BY_TIMER'];
        if (this.pollPromise && runStatuses.indexOf(res.status) !== -1) {
          this.dataSrc.stopPoll(this.pollPromise.__pollId__);
          this.pollPromise = null;
        }
      },
      (err) => {
        if (this.pollPromise) {
          this.dataSrc.stopPoll(this.pollPromise.__pollId__);
          this.pollPromise = null;
        }
        console.log('ERROR: ', err);
      });
  };

  this.pollForMetadata = () => {
    this.pollPromise = this.dataSrc.poll({
      _cdapPath: '/metrics/query',
      method: 'POST',
      body: MyMetricsQueryHelper.constructQuery(
        'qid',
        MyMetricsQueryHelper.contextToTags(this.apiSettings.metric.context),
        this.apiSettings.metric
      )
    },
    (res) => {
      $scope.metadata = res;
      $scope.sliderBarPositionRefresh = LogViewerStore.getState().startTime;
      $scope.initialize();
      this.getStatus();
    }, (err) => {
      // FIXME: We need to fix this. Right now this fails and we need to handle this more gracefully.
      $scope.initialize();
      console.log('ERROR: ', err);
    });
  };

  LogViewerStore.subscribe(() => {
    let state = LogViewerStore.getState();
    if(this.screenSize !== state.fullScreen){
      this.screenSize = state.fullScreen;
      $timeout($scope.initialize);
    }

    //Keep the slider handle in sync with the api call
    if(typeof $scope.updateSliderHandle !== 'undefined'){
      $scope.updateSliderHandle(state.startTime);

      //Check if the pinScrollPosition is less than the value of the query handle
      this.pinScrollPosition = state.scrollPosition;
      if(typeof $scope.updatePin !== 'undefined'){
        $scope.pinScrollingPosition = this.pinScrollPosition;
        $scope.updatePin();
      }

      if($scope.searchResultTimes !== state.searchResults){
        $scope.searchResultTimes = state.searchResults;
        $scope.renderSearchCircles($scope.searchResultTimes);
      }
    }
  });

  if (!this.namespaceId || !this.appId || !this.programType || !this.programId || !this.runId) {
    this.setDefaultTimeWindow();
    return;
  }

  myLogsApi.getLogsMetadata({
    namespace : this.namespaceId,
    appId : this.appId,
    programType : this.programType,
    programId : this.programId,
    runId : this.runId
  }).$promise.then(
    (res) => {
      $scope.metadata = res;
      if(res.start === res.end){
        res.end++;
      }
      this.apiSettings.metric.startTime = res.start;
      this.apiSettings.metric.endTime = res.end;
      $scope.renderSearchCircles([]);
      this.pollForMetadata();
    },
    (err) => {
      this.setDefaultTimeWindow();
      console.log('ERROR: ', err);
    });
}

angular.module(PKG.name + '.commons')
.controller('TimelineController', TimelineController);
