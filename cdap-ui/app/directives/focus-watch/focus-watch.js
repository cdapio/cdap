angular.module(PKG.name + '.commons')
  .directive('myFocusWatch', function($timeout) {
    return {
      scope: {
        model: '=myFocusWatch'
      },
      link: function (scope, element) {

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
