angular.module(PKG.name + '.commons')
  .directive('myMetricPicker', function ($log, $q, MyDataSource) {

    var dSrc = new MyDataSource();
    $log.log('dSrc', dSrc);
    window.dSrc = dSrc;


    function MetricPickerCtrl ($scope) {

      var a = ['system', 'user'];
      $scope.available = {
        types: a
      };

      $scope.metric = {
        type: a[0]
      };

      $scope.getContext = function (v) {
        $log.log('getContext', v);
        return dSrc.request({
          _cdapNsPath: ''
        });
      };

    }

    return {
      restrict: 'E',

      scope: {
        model: '='
      },

      templateUrl: 'metric-picker/metric-picker.html',

      controller: MetricPickerCtrl,

      link: function (scope, elem, attr) {
        $log.log('link', scope.model, elem);
      }
    };
  });
