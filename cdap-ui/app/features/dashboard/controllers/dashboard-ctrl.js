/**
 * DashboardCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardCtrl',
function ($scope, $state, $dropdown, rDashboardsModel) {

  $scope.dashboards = rDashboardsModel.data;

  if(!rDashboardsModel.data.length) {
    rDashboardsModel.add();
  }

  $scope.$watch('dashboards.activeIndex', function (newVal) {
    $state.go($state.current, {tab:newVal});
  });

  function checkTabParam() {
    var tab = parseInt($state.params.tab, 10) || 0;
    if((tab<0 || tab>=$scope.dashboards.length)) {
      tab = 0;
    }
    if($scope.dashboards.activeIndex !== tab) {
      $scope.dashboards.activeIndex = tab;
    }
    $scope.currentBoard = rDashboardsModel.current();
  }

  $scope.$on('$stateChangeSuccess', checkTabParam);
  checkTabParam();



  /**
   * show a dropdown when clicking on the tab of active dashboard
   * @TODO make a directive instead
   */
  $scope.activeTabClick = function (event) {

    var toggle = angular.element(event.target);
    if(!toggle.hasClass('dropdown-toggle')) {
      toggle = toggle.parent();
    }

    if(toggle.parent().hasClass('open')) {
      return;
    }

    var scope = $scope.$new(),
        dd = $dropdown(toggle, {
          template: 'assets/features/dashboard/partials/tab-dd.html',
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
      $state.go($state.current, {tab: 0}, {reload: true});
    });
  };

  $scope.removeDashboard = function () {
    rDashboardsModel.remove($scope.dashboards.activeIndex);
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



});

