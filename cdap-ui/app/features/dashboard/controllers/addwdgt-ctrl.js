/**
 * DashboardAddWdgtCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardAddWdgtCtrl',
function ($scope, $modalInstance, caskFocusManager, Widget) {

  function generateHash (str){
    var hash = 0;
    if (str.length === 0) return hash;
    for (var i = 0; i < str.length; i++) {
      var char = str.charCodeAt(i);
      hash = ((hash<<5)-hash)+char;
      hash = hash & hash; // Convert to 32bit integer
    }
    return hash;
  }

  caskFocusManager.focus('addWdgtType');

  $scope.model = new Widget();

  $scope.widgetTypes = [
    { name: 'Histogram (bar)',       type: 'bar' },
    { name: 'Timeseries (line)',     type: 'line' },
    { name: 'Pie Chart',             type: 'pie' },
    { name: 'Debug',                 type: 'json' }
  ];

  $scope.$watch('model.metric.name', function (newVal) {
    if(newVal) {
      $scope.model.title = newVal;
    }
  });

  $scope.doAddWidget = function () {

    var classes = [
      'chart-1',
      'chart-2',
      'chart-3',
      'chart-4',
      'chart-5',
      'chart-6',
      'chart-7',
      'chart-8',
      'chart-9',
      'chart-10',
      'chart-11'
    ];

    if ($scope.model.metric.addAll) {
      var widgets = [];
      // If the user chooses 'Add All' option, add all the metrics in the current context.
      angular.forEach($scope.model.metric.allMetrics, function(value) {
        widgets.push(
          new Widget({
            type: $scope.model.type,
            title: $scope.model.metric.title,
            color: classes[(generateHash(value) * 13) % classes.length],
            metric: {
              context: $scope.model.metric.context,
              names: [value],
              name: value
            }
          })
        );
      });
      $scope.currentDashboard.addWidget(widgets);
    } else {
      $scope.model.color = classes[(generateHash($scope.model.metric.name) * 13) % classes.length];
      $scope.currentDashboard.addWidget($scope.model);
    }
    $scope.$close();
  };

  $scope.closeModal = function() {
    $modalInstance.close();
  };

});
