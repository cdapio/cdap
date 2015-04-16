angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunDetailController', function($scope) {
    $scope.$on('$destroy', function(event) {
      event.targetScope.runs.selected = null;
    });
  });
