angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsDetailController', function($scope, $state, myWorkFlowApi) {
    var params = {
          appId: $state.params.appId,
          workflowId: $state.params.programId
        };

    myWorkFlowApi.runs(angular.extend({status: 'running'}, params), function(res) {

      $scope.runs = res;
      var count = 0;
      angular.forEach(res, function(runs) {
        if (runs.status === 'RUNNING') {
          count += 1;
        }
      });

      $scope.activeRuns = count;
    });


    $scope.activeRuns = 0;
    $scope.runs = null;

    myWorkFlowApi.status(params, function(res) {
      $scope.status = res.status;
    });

    $scope.toggleFlow = function(action) {
      $scope.status = action;
      myWorkFlowApi[action](params);
    };

});
