var params;
class WorkFlowsRunDetailLogController {

  constructor($scope, myWorkFlowApi, $state, $timeout) {
    this.myWorkFlowApi = myWorkFlowApi;
    this.$timeout = $timeout;

    params = {
      namespace: $state.params.namespace,
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

    this.loadingNext = true;
    this.myWorkFlowApi.prevLogs(params)
      .$promise
      .then( res => {
        this.logs = res;
        this.loadingNext = false;
      });
  }

  loadNextLogs () {
    if (this.logs.length < params.max) {
      return;
    }
    this.loadingNext = true;
    params.fromOffset = this.logs[this.logs.length-1].offset;

    this.myWorkFlowApi.logs(params)
      .$promise
      .then( res => {
        this.logs = _.uniq(this.logs.concat(res));
        this.loadingNext = false;
      });
  }

  loadPrevLogs () {
    if (this.loadingPrev) {
      return;
    }

    this.loadingPrev = true;
    params.fromOffset = this.logs[0].offset;

    this.myWorkFlowApi.prevLogs(params)
      .$promise
      .then( res => {
        this.logs = _.uniq(res.concat(this.logs));
        this.loadingPrev = false;

        this.$timeout(function() {
          document.getElementById(params.fromOffset).scrollIntoView();
        });
      });
  }

}

WorkFlowsRunDetailLogController.$inject = ['$scope', 'myWorkFlowApi', '$state', '$timeout'];
angular.module(`${PKG.name}.feature.workflows`)
  .controller('WorkFlowsRunDetailLogController', WorkFlowsRunDetailLogController);
