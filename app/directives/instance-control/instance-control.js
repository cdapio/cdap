angular.module(PKG.name + '.commons')
  .directive('myInstanceControl', function ($timeout) {

    return {
      restrict: 'E',

      scope: {
        model: '='
      },

      templateUrl: 'instance-control/instance-control.html',

      link: function (scope, element, attrs) {
        scope.processing = false;

        scope.handleSet = function () {
          scope.processing = true;
          
          // Something here to handle http request
          $timeout(function () {
            scope.model.provisioned = scope.model.requested;
            scope.processing = false;
          }, 1000);
        }
      }
    };
  });

