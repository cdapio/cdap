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
    { name: 'Debug',                 type: 'json' }
  ];

  $scope.$watch('model.metric.name', function (newVal) {
    if(newVal) {
      $scope.model.title = newVal;
    }
  });

  $scope.doAddWidget = function () {
    var widgets = [];
    if ($scope.model.metric.addAll) {
      // If the user chooses 'Add All' option, add all the metrics in the current context.
      angular.forEach($scope.model.metric.allMetrics, function(value) {
        widgets.push(
          new Widget({
            type: $scope.model.type,
            title: value,
            metric: {
              context: $scope.model.metric.context,
              name: value
            }
          })
        );
      });
      $scope.currentDashboard.addWidget(widgets);
    } else {
      $scope.currentDashboard.addWidget($scope.model);
    }
    $scope.$close();
  };

});
