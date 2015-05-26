/**
 * DashboardAddWdgtCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardAddWdgtCtrl',
function ($scope, $modalInstance, caskFocusManager, Widget) {

  caskFocusManager.focus('addWdgtType');

  $scope.model = new Widget();

  $scope.widgetTypes = [
    { name: 'Line',                  type: 'c3-line'},
    { name: 'Bar',                   type: 'c3-bar'},
    { name: 'Pie',                   type: 'c3-pie'},
    { name: 'Donut',                 type: 'c3-donut'},
    { name: 'Scatter',               type: 'c3-scatter'},
    { name: 'Spline',                type: 'c3-spline'},
    { name: 'Area',                  type: 'c3-area'},
    { name: 'Area Spline',           type: 'c3-area-spline'},
    { name: 'Area Spline Stacked',   type: 'c3-area-spline-stacked'},
    { name: 'Area Step',             type: 'c3-area-step'},
    { name: 'Step',                  type: 'c3-step'},
    { name: 'Table',                 type: 'table' }
  ];

  $scope.$watch('model.metric.name', function (newVal) {
    if(newVal) {
      $scope.model.title = newVal;
    }
  });

  $scope.doAddWidget = _.once(function () {

    if ($scope.model.metric.addAll) {
      var widgets = [];
      // If the user chooses 'Add All' option, add all the metrics in the current context.
      angular.forEach($scope.model.metric.allMetrics, function(value) {
        widgets.push(
          new Widget({
            type: $scope.model.type,
            title: $scope.model.metric.title,
            metric: {
              context: $scope.model.metric.context,
              names: [value],
              name: value
            }
          })
        );
      });
      $scope.currentBoard.addWidget(widgets);
    } else {
      $scope.currentBoard.addWidget($scope.model);
    }
    $scope.$close();
  });

  $scope.closeModal = function() {
    $modalInstance.close();
  };

});
