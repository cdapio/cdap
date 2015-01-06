/**
 * DashboardCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardCtrl',
function ($scope, $state, $alert, $dropdown, myDashboardsModel, Widget) {

  $scope.dashboards = myDashboardsModel.data;


  /**
   * handle tab navigation
   */
  $scope.$watch('dashboards.activeIndex', function (newVal) {
    $state.go($state.current, {tab:newVal});
  });

  $scope.$on('$stateChangeSuccess', function (event, state) {
    var tab = parseInt($state.params.tab, 10) || 0;
    if((tab<0 || tab>=$scope.dashboards.length)) {
      tab = 0;
    }
    $scope.dashboards.activeIndex = tab;
  });





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


  $scope.addDashboard = function () {
    myDashboardsModel.add({
      title: 'new dashboard'
    });
  };

  $scope.removeDashboard = function () {
    myDashboardsModel.remove($scope.dashboards.activeIndex);
  };

  $scope.renameDashboard = function (newName) {
    myDashboardsModel.current().rename(newName);
  };


  $scope.removeWidget = function (w) {
    myDashboardsModel.current().removeWidget(w);
  };

  $scope.renameWidget = function (w, newName) {
    myDashboardsModel.current().renameWidget(w, newName);
  };



  $scope.dragdrop = {
    dragStart: function (drag) {
      console.log('dragStart', drag.source, drag.dest);
    },
    dragEnd: function (drag) {
      console.log('dragEnd', drag.source, drag.dest);
    }
  };



});

