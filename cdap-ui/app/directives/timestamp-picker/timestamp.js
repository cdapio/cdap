angular.module(PKG.name + '.commons')
  .config(function($datepickerProvider, $timepickerProvider) {
    angular.extend($datepickerProvider.defaults, {
      iconLeft: 'fa fa-chevron-left',
      iconRight: 'fa fa-chevron-right'
    });
    angular.extend($timepickerProvider.defaults, {
      iconUp: 'fa fa-chevron-up',
      iconDown: 'fa fa-chevron-down'
    });
  })
  .directive('myTimestampPicker', function () {
    return {
      restrict: 'E',
      require: 'ngModel',
      scope: {},
      templateUrl: 'timestamp-picker/datetime.html',

      link: function(scope, element, attrs, ngModel) {

        scope.label = attrs.label || 'Timestamp';

        ngModel.$render = function () {
          scope.timestamp = ngModel.$viewValue;
        };

        scope.$watch('timestamp', function (newVal) {
          ngModel.$setViewValue(newVal);
        });
      }
    };
  });
