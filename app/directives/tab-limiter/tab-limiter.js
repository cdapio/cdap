angular.module(PKG.name + '.commons')
  .directive('myTabLimiter', function () {
    return {
      restrict: 'A',
      link: function (scope, element, attrs) {

        console.log(element.prop('offsetWidth'));

        scope.$watch('$panes', function() {
          console.log('panes changed');
        });

      }
    };
  });
