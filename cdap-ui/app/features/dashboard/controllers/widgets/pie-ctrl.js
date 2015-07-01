angular.module(PKG.name+'.feature.dashboard')
.controller('WidgetPieCtrl', function ($scope, $alert) {

  $alert({
    content: 'pie chart using fake data',
    type: 'warning'
  });

  $scope.pieChartData = [
    { label: 'Slice 1', value: 10 },
    { label: 'Slice 2', value: 20 },
    { label: 'Slice 3', value: 40 },
    { label: 'Slice 4', value: 30 }
  ];

});
