angular.module(PKG.name + '.feature.flows')
  .controller('FlowsFlowletDetaiController', function($state, $scope) {

    $scope.flowletId = $state.params.flowletId;

  });
