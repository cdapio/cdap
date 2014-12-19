/**
 * DashboardAddWdgtCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardAddWdgtCtrl',
function ($scope, caskFocusManager, myDashboardsModel, Widget) {

  caskFocusManager.focus('addWdgtType');

  $scope.model = new Widget();

  $scope.model.metric = '/metrics/system/collect.events?start=now-60s&end=now';


  var P = '/assets/features/dashboard/widgets';
  $scope.widgetTypes = [
    { name: 'Welcome',        partial: P+'/welcome.html' },
    { name: 'Histogram',      partial: P+'/bar.html' },
    { name: 'Line Chart',     partial: P+'/line.html' },
    { name: 'Pie Chart',      partial: P+'/pie.html' }
  ];


  $scope.doAddWidget = function () {
    myDashboardsModel.current().addWidget($scope.model);
    $scope.$hide();
  };

});

