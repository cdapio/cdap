/**
 * DashboardCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('UserDashboardCtrl',
function ($scope, $state, $dropdown, rDashboardsModel, MY_CONFIG, $alert, DashboardHelper) {


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

        $element.height = (resizedHeight < 300 ? 200: resizedHeight - 70);
        // Probably need to revisit this if the user wants to view a chart in column
        $element.width = (resizedWidth < 450? 370: resizedWidth - 32);
      } // optional callback fired when item is finished resizing
    }
  };

  if (!$scope.currentBoard) {
    $scope.unknownBoard = true;
  }


  $scope.$on('$destroy', function() {
    DashboardHelper.stopPollDashboard($scope.currentBoard);
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
  $scope.updateWithTimeRange = function() {
    var millisecondsPerDay = 1000*60*60*24;
    var limitInDays = 30;
    var timeRange = $scope.timeOptions.endMs - $scope.timeOptions.startMs;
    if (timeRange >  limitInDays * millisecondsPerDay) {
      // Note: alternative is to interpolate the many many points we get from the backend (mostly will be 0s?).
      $alert({
        content: 'Please choose a shorter time range. Current time range limit is ' + limitInDays + ' days.',
        type: 'warning'
      });
      return;
    }
    $scope.liveDashboard = false;
    DashboardHelper.stopPollDashboard($scope.currentBoard);

    applyOnWidgets(rDashboardsModel, function (widget) {
      widget.metric.startTime = Math.floor($scope.timeOptions.startMs / 1000);
      widget.metric.endTime = Math.floor($scope.timeOptions.endMs / 1000);
      widget.metric.resolution = 'auto';
      widget.settings.isLive = false;
    });

    DashboardHelper.fetchDataDashboard($scope.currentBoard);
  };
  $scope.updateWithTimeRange(); // getting initial value for graph

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

  $scope.stopPolling = function() {
    $scope.liveDashboard = false;
    applyOnWidgets(rDashboardsModel, function (widget) {
      widget.settings.isLive = false;
    });

    DashboardHelper.stopPollDashboard($scope.currentBoard);
  };
});
