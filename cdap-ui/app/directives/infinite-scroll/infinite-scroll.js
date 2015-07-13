angular.module(PKG.name+'.commons')
.directive('infiniteScroll', function ($timeout, EventPipe) {
  return {
    restrict: 'A',
    link: function (scope, elem, attrs) {
      // wrapping in timeout to have content loaded first and setting the scroll to the top
      $timeout(function() {
        elem.prop('scrollTop', 10);
        elem.bind('scroll', function () {
          if (elem.prop('scrollTop') + elem.prop('offsetHeight') >= elem.prop('scrollHeight')) {
            scope.$apply(attrs.infiniteScrollNext);
          } else if (elem.prop('scrollTop') === 0) {
            scope.$apply(attrs.infiniteScrollPrev);
          }
        });
      }, 100);

      scope.$on('$destroy', function () {
        elem.unbind('scroll');
      });
    }
  };

});
