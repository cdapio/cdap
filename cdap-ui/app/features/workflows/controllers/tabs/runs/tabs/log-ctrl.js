angular.module(PKG.name + '.feature.workflows')
  .controller('WorkFlowsRunDetailLogController', function($scope, myWorkFlowApi, $state) {
    var params = {
      appId: $state.params.appId,
      workflowId: $state.params.programId,
      runId: $scope.runs.selected.runid,
      scope: $scope
    };

    $scope.logs = [];
    if (!$scope.runs.length) {
      return;
    }
    params.max = 50;
    myWorkFlowApi.logs(params)
      .$promise
      .then(function(res) {
        $scope.logs = res;
      });
});
