angular.module(PKG.name + '.commons')
  .directive('myFocusWatch', function($timeout, $parse) {
    return {
      scope: {
        model: '=myFocusWatch'
      },
      link: function (scope, element, attrs) {

        scope.$watch('model', function () {
          if (scope.model) {
            $timeout(function() {
              element[0].focus();
            });
          }
        });
      }
    };
  });
