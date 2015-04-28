angular.module(PKG.name + '.commons')
  .directive('myDurationPicker', function () {
    return {
      restrict: 'E',
      require: 'ngModel',
      scope: {},
      templateUrl: 'duration-picker/duration-picker.html',

      link: function(scope, element, attrs, ngModel) {
        // Note: the 'timerange' field must be in a format accepted by the Metrics System
        scope.durationTypes = [
          {name: '1 Minute', startTime: 'now-1m'},
          {name: '1 Hour',   startTime: 'now-1h'},
          {name: '1 Day',    startTime: 'now-1d'},
        ];

        scope.durationType = scope.durationTypes[0];

        // NOTE: we can remove this if we don't want to show an icon next to Time Range selector (past X minutes)
        scope.label = attrs.label || 'Duration';

        ngModel.$render = function () {
          scope.timestamp = ngModel.$viewValue;
        };

        scope.$watch('durationType', function (newVal, oldVal, scope) {
          ngModel.$setViewValue(scope.durationType.startTime);
        });
      }
    };
  });
