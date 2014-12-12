/**
 * DashboardCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardCtrl',
function ($scope) {

  $scope.columns = [];
  var c;
  for (var i = 0; i < 3; i++) {
    c = {items:[]};
    for (var j = 0; j < 2; j++) {
      c.items.push(i+'/'+j);
    }
    $scope.columns.push(c);
  }

  $scope.dragdrop = {
    dragStart: function (drag) {
      console.log('dragStart', drag.source, drag.dest);
    },
    dragEnd: function (drag) {
      console.log('dragEnd', drag.source, drag.dest);
    }
  };

});

