/**
 * DashboardAddWdgtCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardAddWdgtCtrl',
function ($scope, caskFocusManager, Widget) {

  caskFocusManager.focus('addWdgtType');

  $scope.model = new Widget();

  $scope.widgetTypes = [
    { name: 'Timeseries (line)',     type: 'line' },
    { name: 'Histogram (bar)',       type: 'bar' },
    { name: 'Timeseries (area)',     type: 'area' },
    { name: 'Pie Chart',             type: 'pie' },
    // { name: 'Welcome',               type: 'welcome' },
    { name: 'Debug',                 type: 'json' }
  ];

  $scope.$watch('model.metric.name', function (newVal) {
    if(newVal) {
      $scope.model.title = newVal;
    }

  });

  $scope.doAddWidget = function () {
    $scope.currentDashboard.addWidget($scope.model);
    $scope.$hide();
  };

});

