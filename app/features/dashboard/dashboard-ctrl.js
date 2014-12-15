/**
 * DashboardCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardCtrl',
function ($scope, $state) {


  $scope.dashboards = ['First','Second'].map(function (t, k){

    var c, columns = [];
    if (k) {
      columns.push({
        items:['single widget']
      });
    }
    else {
      for (var i = 0; i < 3; i++) {
        c = {items:[]};
        for (var j = 0; j < 2; j++) {
          c.items.push(i+'/'+j);
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




  $scope.dragdrop = {
    dragStart: function (drag) {
      console.log('dragStart', drag.source, drag.dest);
    },
    dragEnd: function (drag) {
      console.log('dragEnd', drag.source, drag.dest);
    }
  };



});

