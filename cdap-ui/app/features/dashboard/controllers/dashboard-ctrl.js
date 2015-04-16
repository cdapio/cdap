/**
 * DashboardCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardCtrl',
function ($scope, $state, $dropdown, rDashboardsModel, MY_CONFIG) {

  $scope.unknownBoard = false;
  $scope.isEnterprise = MY_CONFIG.isEnterprise;
  $scope.dashboards = rDashboardsModel.data || [];
  $scope.liveDashboard = null;
  $scope.startMs = null;
  $scope.endMs = null;
  $scope.durationMs = null;
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

  $scope.makeItLive = function() {
    $scope.liveDashboard = !$scope.liveDashboard;
    var currentColumns = rDashboardsModel.current().columns,
        i, j;
    for (i=0; i<currentColumns.length; i++) {
      for (j=0; j<currentColumns[i].length; j++) {
        currentColumns[i][j].isLive = $scope.liveDashboard;
      }
    }
  };
  $scope.$watch('startMs', function(newVal) {
    var currentColumns = rDashboardsModel.current().columns,
        i, j;
    for (i=0; i<currentColumns.length; i++) {
      for (j=0; j<currentColumns[i].length; j++) {
        currentColumns[i][j].startMs = newVal;
      }
    }
  });

  $scope.$watch('endMs', function(newVal) {
    var currentColumns = rDashboardsModel.current().columns,
        i, j;
    for (i=0; i<currentColumns.length; i++) {
      for (j=0; j<currentColumns[i].length; j++) {
        currentColumns[i][j].endMs = newVal;
      }
    }
  });

$scope.$watch('durationMs', function(newVal) {
    var currentColumns = rDashboardsModel.current().columns,
        i, j;
    for (i=0; i<currentColumns.length; i++) {
      for (j=0; j<currentColumns[i].length; j++) {
        currentColumns[i][j].durationMs = newVal;
      }
    }
  });

});
