/**
 * DashboardAddWdgtCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardAddWdgtCtrl',
function ($scope, caskFocusManager, myDashboardsModel, Widget) {

  caskFocusManager.focus('addWdgtType');

  $scope.model = new Widget();

  $scope.widgetTypes = [
    { name: 'Welcome',               type: 'welcome' },
    { name: 'Histogram (bar)',       type: 'bar' },
    { name: 'Timeseries (line)',     type: 'line' },
    { name: 'Timeseries (area)',     type: 'area' },
    { name: 'Pie Chart',             type: 'pie' },
    { name: 'Debug',                 type: 'json' }
  ];

  $scope.doAddWidget = function () {

    // FIXME - not the place to add metric timespan params
    $scope.model.metric = $scope.model.metric + '&start=now-60s&end=now';

    myDashboardsModel.current().addWidget($scope.model);
    $scope.$hide();
  };

});

