var commonModule = angular.module(PKG.name+'.commons');
commonModule.factory('jsPlumb', function ($window) {
  return $window.jsPlumb;
});

commonModule.directive('myPlumb', function() {
  return {
    restrict: 'E',
    scope: {
      config: '=',
      isDisabled: '=',
      reloaddag: '='
    },
    link: function(scope, element, attrs) {
      scope.element = element;
      scope.getGraphMargins = function (plugins) {
        // Very simple logic for centering the DAG.
        // Should eventually be changed to something close to what we use in workflow/flow graphs.
        var margins = this.element[0].parentElement.getBoundingClientRect();
        var parentWidth = margins.width;
        var noOfNodes = plugins.length;
        var widthOfEachNode = 174;
        var marginLeft = parentWidth - (noOfNodes * 174);
        if (marginLeft < 100){
          marginLeft = -20;
        } else {
          marginLeft = marginLeft/2;
        }
        return {
          left: marginLeft
        };
      };
    },
    templateUrl: 'plumb/my-plumb.html',
    controller: 'MyPlumbController',
    controllerAs: 'MyPlumbController'
  };
});
