var params;
class WorkFlowsRunDetailLogController {

  constructor($scope, myWorkFlowApi, $state) {
    this.myWorkFlowApi = myWorkFlowApi;

    params = {
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

    this.loading = true;
    this.myWorkFlowApi.logs(params)
      .$promise
      .then( res => {
        this.logs = res;
        this.loading = false;
      });
  }

  loadMoreLogs () {
    if (this.logs.length < params.max) {
      return;
    }
    this.loading = true;
    params.max += 50;

    this.myWorkFlowApi.logs(params)
      .$promise
      .then( res => {
        this.logs = res;
        this.loading = false;
      });
  }
}

WorkFlowsRunDetailLogController.$inject = ['$scope', 'myWorkFlowApi', '$state'];
angular.module(`${PKG.name}.feature.workflows`)
  .controller('WorkFlowsRunDetailLogController', WorkFlowsRunDetailLogController);
