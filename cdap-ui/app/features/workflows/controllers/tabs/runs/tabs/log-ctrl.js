'use strict';
class WorkFlowsRunDetailLogController {
  constructor($scope, myWorkFlowApi, $state) {
    let params = {
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
      .then( res => this.logs = res );
  }
}

WorkFlowsRunDetailLogController.$inject = ['$scope', 'myWorkFlowApi', '$state'];
angular.module(PKG.name + '.feature.workflows')
  .controller('WorkFlowsRunDetailLogController', WorkFlowsRunDetailLogController);
