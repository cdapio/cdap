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

angular.module(PKG.name+'.feature.dashboard').controller('DashboardCtrl',
function ($scope, $state, rDashboardsModel, MY_CONFIG, myAlertOnValium, $timeout) {

  $scope.unknownBoard = false;
  $scope.isEnterprise = MY_CONFIG.isEnterprise;
  $scope.dashboards = rDashboardsModel.data || [];
  var tab = parseInt($state.params.tab, 10);
  if (isNaN(tab)) {
    $scope.dashboards.activeIndex = 'system';
  } else {
    $scope.dashboards.activeIndex = tab;
  }
  /**
   * show a dropdown when clicking on the tab of active dashboard
   * @TODO make a directive instead
   */
  $scope.activeTabClick = function (event, index) {
    if (index === 'system') {
      $scope.unknownBoard = true;
      $state.go('dashboard.standard.cdap');
      $scope.dashboards.activeIndex = 'system';
      return;
    }

    if (index !== $scope.dashboards.activeIndex || !$state.includes('dashboard.user')) {
      $scope.unknownBoard = true;
      $state.go('dashboard.user', {
        tab: index
      });
      $scope.dashboards.activeIndex = index;
      return;
    }

    $scope.dashboards[index].isopen = !$scope.dashboards[index].isopen;
    event.preventDefault();
    event.stopPropagation();
  };

  $scope.addWidget = function () {
    if (!rDashboardsModel.current().canAddWidget()) {
      myAlertOnValium.show({
        content: 'Cannot add more than ' + rDashboardsModel.current().WIDGET_LIMIT + ' widgets.',
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

  $scope.$on('$stateChangeSuccess', function(event, toState, toParams, fromState) {
    // This might be redundant if the user navigates by clicking on the tabs,
    // but its just re-assignment. This is really useful when the user navigates through
    // the browser's back button or (or BACKSPACE). re-assignment is ok, not assigning proper
    // values is a problem.
    if (
      fromState.name.indexOf('dashboard') !== -1 &&
      toState.name.indexOf('dashboard') !== -1
    ) {
      if ($state.includes('dashboard.standard.*')) {
        $scope.unknownBoard = true;
        $scope.dashboards.activeIndex = 'system';
      } else {
        $scope.dashboards.activeIndex = parseInt(toParams.tab, 10);
      }
    }
  });

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
})
.directive('widgetDdMenu', function() {
  return {
    replace: true,
    restrict: 'E',
    templateUrl: '/assets/features/dashboard/templates/partials/wdgt-dd.html'
  };
});
