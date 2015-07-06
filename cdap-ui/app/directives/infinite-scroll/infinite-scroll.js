angular.module(PKG.name+'.commons')
.directive('infiniteScroll', function ($timeout) {
  return {
    restrict: 'A',
    link: function (scope, elem, attrs) {

      // wrapping in timeout to have content loaded first and setting the scroll to the top
      $timeout(function() {
        elem.prop('scrollTop', 0);
        elem.bind('scroll', function () {
          if (elem.prop('scrollTop') + elem.prop('offsetHeight') >= elem.prop('scrollHeight')) {
            scope.$apply(attrs.infiniteScroll);
          }
        });
      }, 100);

      scope.$on('$destroy', function () {
        elem.unbind('scroll');
      });
    }
  };

});
