angular.module(PKG.name + '.commons')
  .directive('myMetricPicker', function ($log, MyDataSource) {

    var dSrc = new MyDataSource();
    $log.log('dSrc', dSrc);
    window.dSrc = dSrc;

    return {
      restrict: 'E',
      scope: {
        model: '='
      },
      templateUrl: 'metric-picker/metric-picker.html',

      controller: function ($scope) {

        var a = ['system', 'user'];
        $scope.availableTypes = a;
        $scope.metricType = a[0];


      },

      link: function (scope, elem, attr) {
        $log.log('link', scope.model, elem);
      }
    };
  });
