angular.module(PKG.name + '.commons')
  .directive('myTimestampPicker', function () {
    return {
      restrict: 'E',
      require: 'ngModel',
      scope: {},
      replace: true,
      templateUrl: 'timestamp-picker/picker.html',

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
