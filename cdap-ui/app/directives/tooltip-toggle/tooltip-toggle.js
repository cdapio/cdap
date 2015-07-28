angular.module(PKG.name + '.commons')

  .directive('tooltipToggle', function ($timeout) {
    return {
      restrict: 'A',
      link: function (scope, element, attrs) {
        attrs.tooltipTrigger = 'customShow';

        scope.$watch(attrs.tooltipToggle, function (newVal) {
          $timeout(function () {
            if (newVal) {
              element.triggerHandler('customShow');
            } else {
              element.triggerHandler('customHide');
            }
          });
        });
      }
    };
  })

  .directive('tooltipEllipsis', function ($timeout) {
    return {
      restrict: 'A',
      scope: {
        ellipsis: '='
      },
      link: function (scope, element, attrs) {
        function isEllipsisActive(e) {
          return (e[0].offsetWidth > e[0].parentElement.offsetWidth - 20);
        }

        scope.$watch(attrs.tooltipEllipsis, function () {
          if (attrs.tooltipEllipsis) {
            if (isEllipsisActive(element)) {
              scope.ellipsis = true;
            } else {
              scope.ellipsis = false;
            }
          }
        });

      }
    };
  });
