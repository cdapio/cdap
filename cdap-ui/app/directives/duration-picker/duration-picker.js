angular.module(PKG.name + '.commons')
  .directive('myDurationPicker', function () {
    return {
      restrict: 'E',
      require: 'ngModel',
      scope: {},
      templateUrl: 'duration-picker/duration-picker.html',

      link: function(scope, element, attrs, ngModel) {
        scope.durationTypes = [
          'seconds',
          'minutes',
          'hours',
          'days'
        ];

        scope.durationType = 'seconds';

        scope.label = attrs.label || 'Resolution';

        ngModel.$render = function () {
          scope.timestamp = ngModel.$viewValue;
        };

        scope.$watchGroup(['duration', 'durationType'], function (newValues, oldValues, scope) {
          if (!angular.equals(newValues, oldValues)) {
            if (scope.duration < 0) {
              return;
            }

            var timeBack = calculateTimeBack(scope.duration, scope.durationType) * 1000;
            ngModel.$setViewValue(timeBack);
          }
        });

        function calculateTimeBack(amount, units) {
          switch(units) {
            case 'seconds':
              return amount;
            case 'minutes':
              return amount * 60;
            case 'hours':
              return amount * 60 * 60;
            case 'days':
              return amount * 60 * 60 * 24;
          }
        }
      }
    };
  });
