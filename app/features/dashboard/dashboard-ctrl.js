/**
 * DashboardCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardCtrl',
function ($scope, $state, $alert, $dropdown) {


  $scope.dashboards = ['First','Second','Third'].map(function (t, k){

    var c, columns = [];
    if (k===1) {
      columns.push([{
        title: 'full width widget'
      }]);
    }
    else {
      for (var i = 0; i < 3; i++) {
        c = [];
        for (var j = 0; j < 2; j++) {
          c.push({
            title: 'widget #'+(j+1),
            badge: 'c'+i
          });
        }
        columns.push(c);
      }
    }


    return {
      title: t,
      columns: columns
    };
  });


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


  // todo: make a directive
  $scope.activeTabClick = function (event) {

    var toggle = angular.element(event.target);
    if(!toggle.hasClass('dropdown-toggle')) {
      toggle = toggle.parent();
    }

    if(toggle.parent().hasClass('open')) {
      return;
    }

    var scope = $scope.$new();

    var dd = $dropdown(toggle, {
      template: 'assets/features/dashboard/templates/dropdown.html',
      animation: 'am-flip-x',
      trigger: 'manual',
      prefixEvent: 'dashboard-tab-dropdown',
      scope: scope
    });

    dd.$promise.then(function(){
      dd.show();
    });

    scope.$on('dashboard-tab-dropdown.hide', function () {
      dd.destroy();
    });

  };




  $scope.rmWidget = function (wdgt) {
    var d = $scope.dashboards[$scope.dashboards.activeTab];
    angular.forEach(d.columns, function (c, i) {
      d.columns[i] = c.filter(function (p) {
        return wdgt !== p;
      });
    });
  };




  $scope.addDashboard = function () {
    var n = $scope.dashboards.push({
      title: 'new dashboard',
      columns: [[],[],[]]
    });
    $scope.dashboards.activeTab = n-1;
  };


  $scope.rmDashboard = function () {
    $scope.dashboards.splice($scope.dashboards.activeTab, 1);
  };


  $scope.addWidget = function () {
    var columns = $scope.dashboards[$scope.dashboards.activeTab].columns,
        index = 0,
        smallest;

    // find the columns with the least widgets
    for (var i = 0; i < columns.length; i++) {
      var c = columns[i].length;
      if(smallest===undefined || (c < smallest)) {
        smallest = c;
        index = i;
      }
    };

    // add a widget there
    columns[index].unshift({
      title: 'added widget'
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

