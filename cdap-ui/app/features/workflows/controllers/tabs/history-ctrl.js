angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsHistoryController', function($scope, $state, myWorkFlowApi) {
    var params = {
      appId: $state.params.appId,
      workflowId: $state.params.programId,
      scope: $scope
    };
    $scope.runs = [];
    myWorkFlowApi.runs(params, function(res) {
      $scope.runs = res;
    });
  });
