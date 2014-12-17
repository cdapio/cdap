/**
 * DashboardCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardCtrl',
function ($scope, $state, $alert, $dropdown) {


  $scope.dashboards = [
    {
      title: 'grid',
      columns: [[{
        title: 'widget title'
      }],[],[]]
    },
    {
      title: 'full-width',
      columns: [[{
        title: 'hello world'
      }]]
    }
  ];




  /**
   * handle tab navigation
   */
  $scope.$watch('dashboards.activeTab', function (newVal) {
    $state.go($state.includes('**.tab') ? $state.current : '.tab', {tab:newVal});
  });

  $scope.$on('$stateChangeSuccess', function (event, state) {
    var tab = parseInt($state.params.tab, 10) || 0;
    if((tab<0 || tab>=$scope.dashboards.length)) {
      tab = 0;
    }
    $scope.dashboards.activeTab = tab;
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





  /**
   * add a new dashboard tab
   */
  $scope.addDashboard = function () {
    var n = $scope.dashboards.push({
      title: 'new dashboard',
      columns: [[{
        title: 'default widget'
      }],[],[]]
    });
    $scope.dashboards.activeTab = n-1;
  };


  /**
   * remove the currently active dashboard tab
   */
  $scope.removeDashboard = function () {
    $scope.dashboards.splice($scope.dashboards.activeTab, 1);
  };


  /**
   * rename the currently active dashboard tab
   */
  $scope.renameDashboard = function (newName) {
    alert(newName);
  };


  /**
   * remove a widget from the active dashboard tab
   * @param  {object} wdgt the widget object
   */
  $scope.removeWidget = function (wdgt) {
    var d = $scope.dashboards[$scope.dashboards.activeTab];
    angular.forEach(d.columns, function (c, i) {
      d.columns[i] = c.filter(function (p) {
        return wdgt !== p;
      });
    });
  };



  /**
   * add a widget to the active dashboard tab
   */
  $scope.addWidget = function () {
    var columns = $scope.dashboards[$scope.dashboards.activeTab].columns,
        index = 0,
        smallest;

    // find the column with the least widgets
    for (var i = 0; i < columns.length; i++) {
      var c = columns[i].length;
      if(smallest===undefined || (c < smallest)) {
        smallest = c;
        index = i;
      }
    }

    columns[index].unshift({
      title: 'added widget'
    });
  };




  $scope.renameWidget = function () {
    $alert({
      title: 'Sorry!',
      content: 'renameWidget is not yet implemented.',
      type: 'danger'
    });
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

