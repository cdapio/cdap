angular.module(PKG.name + '.feature.flows')
  .controller('FlowsFlowletDetailController', function($state, $scope) {

    $scope.flowletId = $state.params.flowletId;

  });
