// Probably should be renamed or refactored. This is not doing anything apart
// from handling size changes to the widget.
angular.module(PKG.name+'.feature.dashboard')
.controller('C3WidgetTimeseriesCtrl', function ($scope, myHelpers, $timeout) {
  $scope.chartSize = { height: 200 };
  var widget = myHelpers.objectQuery($scope, 'gridsterItem', '$element', 0),
      widgetHeight,
      widgetWidth;
  if (widget) {
    widgetHeight = parseInt(widget.style.height, 10);
    widgetWidth = parseInt(widget.style.width, 10);
    if (widgetHeight > 300) {
      $scope.wdgt.height = widgetHeight - 70;
    }
    $scope.wdgt.width = widgetWidth - 32;
  }

  $scope.$on('gridster-resized', function() {
    $timeout(function() {
      $scope.chartSize.height = parseInt($scope.gridsterItem.$element[0].style.height, 10) - 70;
      $scope.chartSize.width = parseInt($scope.gridsterItem.$element[0].style.width, 10) - 32;
    });
  });

  $scope.$watch('wdgt.height', function(newVal) {
    $scope.chartSize.height = newVal;
  });
  $scope.$watch('wdgt.width', function(newVal) {
    if (!newVal) {
      return;
    }
    $scope.chartSize.width = newVal;
  });

});
