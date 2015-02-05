angular.module(PKG.name + '.feature.flows')
  .controller('FlowsDetailController', function($scope, $state) {
    if (!$state.params.runId) {
      $state.go('flows.detail.runs');
    }
  });
