/**
 * DashboardCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardCtrl',
function ($scope, $state, $dropdown, rDashboardsModel, MY_CONFIG, $alert) {


  $scope.unknownBoard = false;
  $scope.isEnterprise = MY_CONFIG.isEnterprise;
  $scope.dashboards = rDashboardsModel.data || [];
  $scope.liveDashboard = null;

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
  $scope.dashboards.activeIndex = parseInt($state.params.tab, 10) || 0;


  $scope.currentBoard = rDashboardsModel.current();
  if (!$scope.currentBoard) {
    $scope.unknownBoard = true;
  }

  /**
   * show a dropdown when clicking on the tab of active dashboard
   * @TODO make a directive instead
   */
  $scope.activeTabClick = function (event, index) {
    if (index !== $scope.dashboards.activeIndex || !$state.includes('dashboard.user')) {
      return;
    }
    var toggle = angular.element(event.target);
    if(!toggle.hasClass('dropdown-toggle')) {
      toggle = toggle.parent();
    }

    var scope = $scope.$new(),
        dd = $dropdown(toggle, {
          template: 'assets/features/dashboard/templates/partials/tab-dd.html',
          animation: 'am-flip-x',
          trigger: 'manual',
          prefixEvent: 'dashboard-tab-dd',
          scope: scope
        });

    dd.$promise.then(function(){
      dd.show();
    });

    scope.$on('dashboard-tab-dd.hide', function () {
      dd.destroy();
    });

  };

  $scope.addWidget = function () {
    if (!rDashboardsModel.current().canAddWidget()) {
      $alert({
        content: 'Can not add more than ' + rDashboardsModel.current().WIDGET_LIMIT + ' widgets.',
        type: 'warning'
      });
      return;
    }
    $state.go('dashboard.user.addwdgt', {tab: $state.params.tab}, {reload: false});
  }

  $scope.addDashboard = function (title) {
    rDashboardsModel.add({title: title}).then(function() {
      var tabDest = rDashboardsModel.data.length - 1;
      $state.go('dashboard.user', {tab: tabDest}, {reload: true});
    });
  };

  $scope.removeDashboard = function () {
    rDashboardsModel.remove($scope.dashboards.activeIndex)
      .then(function() {
        $state.go('dashboard.standard.cdap', {}, {reload: true});
      });
  };

  $scope.reorderDashboard = function (reverse) {
    var newIndex = rDashboardsModel.reorder(reverse);
    $state.go($state.current, {tab: newIndex}, {reload: true});
  };

  $scope.dragdrop = {
    dragStart: function (drag) {
      console.log('dragStart', drag.source, drag.dest);
    },
    dragEnd: function (drag) {
      console.log('dragEnd', drag.source, drag.dest);
      rDashboardsModel.current().persist();
    }
  };


  function applyOnWidgets(rDashboardsModel, func) {
    var currentColumns = rDashboardsModel.current().columns,
        i, j;
    for (i=0; i<currentColumns.length; i++) {
      for (j=0; j<currentColumns[i].length; j++) {
        func(currentColumns[i][j]);
      }
    }
  }

  $scope.changeColumn = function (n) {
    rDashboardsModel.current().changeColumn(n);
  };

  $scope.toggleDragDrop = function() {
    rDashboardsModel.current().toggleDragDrop();
  };

  $scope.currentColumn = function() {
    return rDashboardsModel.current().numColumn;
  };

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
    applyOnWidgets(rDashboardsModel, function (widget) {
      widget.metric.startTime = Math.floor($scope.timeOptions.startMs / 1000);
      widget.metric.endTime = Math.floor($scope.timeOptions.endMs / 1000);
      widget.metric.resolution = 'auto';
      widget.isLive = false;
      widget.reconfigure();
    });
  };

  $scope.updateWithFrequency = function() {
    applyOnWidgets(rDashboardsModel, function (widget) {
      widget.metric.startTime = $scope.timeOptions.durationMs;
      widget.metric.endTime = 'now';
      widget.metric.resolution = 'auto';
      widget.isLive = true;
      widget.interval = $scope.timeOptions.refreshInterval.value * 1000;
      widget.reconfigure();
    });
  };
});
