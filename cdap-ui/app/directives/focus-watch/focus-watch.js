angular.module(PKG.name + '.commons')
  .directive('myFocusWatch', function($timeout, $parse) {
    return {
      link: function (scope, element, attrs) {
        var model = $parse(attrs.myFocusWatch);

        scope.$watch(model, function (v) {
          if (v) {
            $timeout(function() {
              element[0].focus();
            });
          }
        });
      }
    };
  });
