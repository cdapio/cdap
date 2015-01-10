angular.module(PKG.name + '.commons')
  .directive('myMetricPicker', function ($log) {
    return {
      restrict: 'E',
      scope: {
        model: '='
      },
      templateUrl: 'metric-picker/metric-picker.html',

      controller: function ($scope) {
        $log.log('controller', $scope);
      },

      link: function (scope, elem, attr) {
        $log.log('link', arguments);
      }
    };
  });
