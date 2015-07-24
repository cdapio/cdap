var commonModule = angular.module(PKG.name+'.commons');
commonModule.factory('jsPlumb', function ($window) {
  return $window.jsPlumb;
});

commonModule.directive('myPlumb', function() {
  return {
    restrict: 'E',
    scope: {
      config: '='
    },
    link: function(scope, element, attrs) {
      scope.element = element;
      scope.getGraphMargins = function (plugins) {
        // Very simple logic for centering the DAG.
        // Should eventually be changed to something close to what we use in workflow/flow graphs.
        var margins = this.element[0].parentElement.getBoundingClientRect();
        var parentWidth = margins.width;
        var noOfNodes = plugins.length;
        var widthOfEachNode = 260;
        var finalMargin = parentWidth - (noOfNodes * 260);
        if ( finalMargin >= 100) {
          finalMargin = finalMargin / 2;
        } else if (finalMargin < 100){
          finalMargin = 150;
        }
        return finalMargin;
      };
    },
    templateUrl: 'plumb/my-plumb.html',
    controller: 'MyPlumbController',
    controllerAs: 'MyPlumbController'
  };
});
