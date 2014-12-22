/**
 * DashboardAddWdgtCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardAddWdgtCtrl',
function ($scope, caskFocusManager, myDashboardsModel, Widget) {

  caskFocusManager.focus('addWdgtType');

  $scope.model = new Widget();

  $scope.model.metric = '/metrics/system/collect.events?start=now-60s&end=now';

  $scope.widgetTypes = [
    { name: 'Welcome',               type: 'welcome' },
    { name: 'Histogram (bar)',       type: 'bar' },
    { name: 'Timeseries (line)',     type: 'line' },
    { name: 'Timeseries (area)',     type: 'area' },
    { name: 'Pie Chart',             type: 'pie' },
    { name: 'Debug',                 type: 'json' }
  ];

  $scope.doAddWidget = function () {
    myDashboardsModel.current().addWidget($scope.model);
    $scope.$hide();
  };

});

