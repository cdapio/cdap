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

function TimelinePreviewController ($scope, LogViewerStore, LOGVIEWERSTORE_ACTIONS, myPreviewLogsApi, MyMetricsQueryHelper, MyCDAPDataSource, ProgramsHelpers, moment, $timeout, caskWindowManager, HydratorPlusPlusPreviewStore) {

  let dataSrc = new MyCDAPDataSource($scope);
  this.pinScrollPosition = 0;
  this.screenSize = LogViewerStore.getState().fullScreen;
  this.previewStatus = LogViewerStore.getState().statusInfo.status;
  let queryPoll = null;
  const previewEndedStatuses = ['KILLED', 'COMPLETED', 'FAILED', 'RUN_FAILED', 'STOPPED', 'KILLED_BY_TIMER'];

  this.apiSettings = {
    metric : {
      context: `namespace.${this.namespaceId}.app.${this.previewId}`,
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

  this.updateStatusInStore = function(val) {
    LogViewerStore.dispatch({
      type: LOGVIEWERSTORE_ACTIONS.SET_STATUS,
      payload: {
        status: val
      }
    });
  };

  this.setDefaultTimeWindow = () => {
    this.apiSettings.metric.startTime = '';
    this.apiSettings.metric.endTime = '';
    this.updateStartTimeInStore(this.apiSettings.metric.startTime);
  };

  this.stopPollIfPreviewEnded = () => {
    if (!queryPoll) {
      return;
    }
    if (this.previewStatus && previewEndedStatuses.indexOf(this.previewStatus) !== -1) {
      dataSrc.stopPoll(queryPoll.__pollId__);
      queryPoll = null;
    }
  };

  this.pollForMetadata = () => {
    if (queryPoll) {
      return;
    }

    queryPoll = dataSrc.poll({
      _cdapPath: `/namespaces/${this.namespaceId}/previews/${this.previewId}/metrics/query`,
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
      this.stopPollIfPreviewEnded();
    }, (err) => {
      // FIXME: We need to fix this. Right now this fails and we need to handle this more gracefully.
      $scope.initialize();
      console.log('ERROR: ', err);
    });
  };

  let logViewerStorePoll = LogViewerStore.subscribe(() => {
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

    if (state.statusInfo.status && this.previewStatus !== state.statusInfo.status) {
      this.previewStatus = state.statusInfo.status;
      this.stopPollIfPreviewEnded();
    }
  });

  let previewStorePoll = HydratorPlusPlusPreviewStore.subscribe(() => {
    let state = HydratorPlusPlusPreviewStore.getState().preview;
    if (this.previewId !== state.previewId) {
      this.previewId = state.previewId;
      this.initializeTimeline();
    }
  });

  this.initializeTimeline = () => {
    if (!this.namespaceId || !this.previewId) {
      this.setDefaultTimeWindow();
      return;
    }

    myPreviewLogsApi.getLogsStatus({
      namespace : this.namespaceId,
      previewId : this.previewId
    }).$promise.then(
      (res) => {
        $scope.metadata = res;
        if(res.startTime === res.endTime){
          res.endTime++;
        }
        this.apiSettings.metric.startTime = Math.floor(res.startTime/1000);
        this.apiSettings.metric.endTime = Math.floor(res.endTime/1000);
        $scope.renderSearchCircles([]);
        this.pollForMetadata();
      },
      (err) => {
        this.setDefaultTimeWindow();
        console.log('ERROR: ', err);
      });
  };

  this.initializeTimeline();

  $scope.$on('$destroy', function() {
    if (logViewerStorePoll) {
      logViewerStorePoll();
    }
    if (previewStorePoll) {
      previewStorePoll();
    }
    if (queryPoll) {
      dataSrc.stopPoll(queryPoll.__pollId__);
      queryPoll = null;
    }
  });
}

angular.module(PKG.name + '.commons')
.controller('TimelinePreviewController', TimelinePreviewController);
