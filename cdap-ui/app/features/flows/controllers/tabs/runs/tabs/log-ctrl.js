angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunDetailLogController', function($scope, $state, myFlowsApi) {

    this.logs = [];
    if (!$scope.RunsController.runs.length) {
      return;
    }

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      flowId: $state.params.programId,
      runId: $scope.RunsController.runs.selected.runid,
      max: 50,
      scope: $scope
    };

    myFlowsApi.logs(params)
      .$promise
      .then(function (res) {
        this.logs = res;
      }.bind(this));

  });
