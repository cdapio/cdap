/**
 * DashboardCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardCtrl',
function ($scope, $state, $alert) {


  $scope.dashboards = ['First','Second','Third'].map(function (t, k){

    var c, columns = [];
    if (k===1) {
      columns.push([{
        title: 'single widget'
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
      title: t+' Dashboard',
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


  $scope.rmWidget = function (wdgt) {
    var d = $scope.dashboards[$scope.dashboards.activeTab];
    angular.forEach(d.columns, function (c, i) {
      d.columns[i] = c.filter(function (p) {
        return wdgt !== p;
      });
    });
  };


  $scope.addDashboard = function () {
    $alert({
      title: 'Sorry!',
      content: 'It does not work yet.',
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

