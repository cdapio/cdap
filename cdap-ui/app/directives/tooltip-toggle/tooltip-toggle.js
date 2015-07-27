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
  });
