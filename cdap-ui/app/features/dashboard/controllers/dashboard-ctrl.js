/**
 * DashboardCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardCtrl',
function ($scope, $state, $dropdown, rDashboardsModel, MY_CONFIG, $alert, $timeout) {

  $scope.unknownBoard = false;
  $scope.isEnterprise = MY_CONFIG.isEnterprise;
  $scope.dashboards = rDashboardsModel.data || [];
  var tab = parseInt($state.params.tab, 10);
  if (isNaN(tab)) {
    $scope.dashboards.activeIndex = 'system';
  } else {
    $scope.dashboards.activeIndex = tab;
  }
  var dropdown;
  /**
   * show a dropdown when clicking on the tab of active dashboard
   * @TODO make a directive instead
   */
  $scope.activeTabClick = function (event, index) {
    if (index == 'system') {
      $scope.unknownBoard = true;
      $state.go('dashboard.standard.cdap');
      $scope.dashboards.activeIndex = 'system';
      return;
    }

    if (index !== $scope.dashboards.activeIndex || !$state.includes('dashboard.user')) {
      $scope.unknownBoard = true;
      $state.go('dashboard.user', {tab: index});
      $scope.dashboards.activeIndex = index;
      return;
    }

    $scope.dashboards[index].isopen = !$scope.dashboards[index].isopen;
    event.preventDefault();
    event.stopPropagation();
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
  };

  $scope.addDashboard = _.throttle(function (title) {
    rDashboardsModel.add({title: title}).then(function() {
      var tabDest = rDashboardsModel.data.length - 1;
      $state.go('dashboard.user', {tab: tabDest}, {reload: true});
    });
  }, 2000, {'trailing': false});

  $scope.removeDashboard = function () {
    rDashboardsModel.remove($scope.dashboards.activeIndex)
      .then(function() {
        $state.go('dashboard.standard.cdap', {}, {reload: true});
      });
  };

  $scope.reorderDashboard = function (reverse) {
    var newIndex = rDashboardsModel.reorder(reverse);
    if (newIndex > 0) {
      $timeout(function() {
        $state.go($state.current, {tab: newIndex}, {reload: true});
      });
    }
  };

})
.directive('tabDdMenu', function() {
    return {
        replace: true,
        restrict: 'E',
        templateUrl: '/assets/features/dashboard/templates/partials/tab-dd.html'
    };
});
