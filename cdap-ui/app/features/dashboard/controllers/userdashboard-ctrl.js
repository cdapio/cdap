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

/**
 * DashboardCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('UserDashboardCtrl',
function ($scope, $state, $dropdown, rDashboardsModel, MY_CONFIG, myAlertOnValium, DashboardHelper) {

  $scope.unknownBoard = false;
  $scope.liveDashboard = false;

  // Available refresh rates.
  $scope.refreshIntervals = [
    { name: '5 seconds',  value: 5 },
    { name: '10 seconds', value: 10 },
    { name: '30 seconds', value: 30 },
    { name: '1 minute',   value: 60 },
    { name: '5 minutes',  value: 300 },
  ];

  var now = Date.now();
  $scope.timeOptions = { startMs : now - 60 * 1000,
                         endMs   : now,
                         refreshInterval: $scope.refreshIntervals[1] };

  $scope.currentBoard = rDashboardsModel.current();

  $scope.gridsterOpts = {
    rowHeight: '280',
    columns: 6,
    minSizeX: 2,
    mobileBreakPoint: 800,
    swapping: true,
    resizable: {
      enabled: true,
      stop: function(event, uiWidget, $element) {
        var resizedHeight = parseInt(uiWidget[0].style.height, 10),
            resizedWidth = parseInt(uiWidget[0].style.width, 10);
        if ($element) {
          $element.height = (resizedHeight < 300 ? 200: resizedHeight - 70);
          // Probably need to revisit this if the user wants to view a chart in column
          $element.width = (resizedWidth < 450? 370: resizedWidth - 32);
        }
      } // optional callback fired when item is finished resizing
    }
  };

  $scope.$on('$destroy', function() {
    if ($scope.liveDashboard) {
      DashboardHelper.stopPollDashboard($scope.currentBoard);
    }
  });

  function applyOnWidgets(rDashboardsModel, func) {
    var currentColumns = rDashboardsModel.current().columns,
        i;
    for (i=0; i<currentColumns.length; i++) {
      func(currentColumns[i]);
    }
  }

  $scope.activateTab = function(idx) {
    $scope.currentTab = idx;
  };
  $scope.tabs = ['Time Range', 'Frequency'];
  $scope.currentTab = 1;

  // TODO: new widgets added won't have the properties set below
  $scope.updateWithTimeRange = function(isStopPoll) {
    var millisecondsPerDay = 1000*60*60*24;
    var limitInDays = 30;
    var timeRange = $scope.timeOptions.endMs - $scope.timeOptions.startMs;
    if (timeRange >  limitInDays * millisecondsPerDay) {
      // Note: alternative is to interpolate the many many points we get from the backend (mostly will be 0s?).
      myAlertOnValium.show({
        content: 'Please choose a shorter time range. Current time range limit is ' + limitInDays + ' days.',
        type: 'warning'
      });
      return;
    }
    if (isStopPoll !== false) {
      $scope.liveDashboard = false;
      DashboardHelper.stopPollDashboard($scope.currentBoard);
    }

    applyOnWidgets(rDashboardsModel, function (widget) {
      widget.metric.startTime = Math.floor($scope.timeOptions.startMs / 1000);
      widget.metric.endTime = Math.floor($scope.timeOptions.endMs / 1000);
      widget.metric.resolution = 'auto';
      widget.settings.isLive = false;
    });

    DashboardHelper.fetchDataDashboard($scope.currentBoard);
  };
  $scope.updateWithTimeRange(false); // getting initial value for graph

  $scope.updateWithFrequency = function() {
    $scope.liveDashboard = true;
    applyOnWidgets(rDashboardsModel, function (widget) {
      widget.metric.startTime = $scope.timeOptions.durationMs;
      widget.metric.endTime = 'now';
      widget.metric.resolution = 'auto';
      widget.settings.isLive = true;
      widget.settings.interval = $scope.timeOptions.refreshInterval.value * 1000;
    });

    DashboardHelper.startPollDashboard($scope.currentBoard);
  };

  $scope.$watch('timeOptions.durationMs', function() {
    if ($scope.liveDashboard) {
      $scope.updateWithFrequency();
    }
  });
  $scope.updateRefreshInterval =  function() {
    if ($scope.liveDashboard) {
      $scope.updateWithFrequency();
    }
  };

  $scope.stopPolling = function() {
    $scope.liveDashboard = false;
    applyOnWidgets(rDashboardsModel, function (widget) {
      widget.settings.isLive = false;
    });

    DashboardHelper.stopPollDashboard($scope.currentBoard);
  };
});
