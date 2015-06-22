angular.module(PKG.name + '.feature.workflows')
  .controller('WorkFlowsRunDetailLogController', function($scope, myWorkFlowApi, $state) {
    var params = {
      appId: $state.params.appId,
      workflowId: $state.params.programId,
      runId: $scope.RunsController.runs.selected.runid,
      scope: $scope,
      max: 50
    };

    this.logs = [];
    if (!$scope.RunsController.runs.length) {
      return;
    }
    myWorkFlowApi.logs(params)
      .$promise
      .then(function(res) {
        this.logs = res;
      }.bind(this));
});
