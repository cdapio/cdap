angular.module(PKG.name + '.commons')
  .directive('myMetricPicker', function () {
    return {
      restrict: 'E',
      scope: {
        model: '='
      },
      templateUrl: 'metric-picker/metric-picker.html'
    };
  });
