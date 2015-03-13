angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsDetailController', function($scope, $state, myWorkFlowApi) {
    var params = {
          appId: $state.params.appId,
          workflowId: $state.params.programId
        };

    var prom = myWorkFlowApi.runs(angular.extend({status: 'running'}, params))
    prom.$promise.then(function(res) {
        console.info("Getting Updates: ", res);
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

    $scope.$on('$destroy', function() {
      myWorkFlowApi.runsStop(params);
      myWorkFlowApi.statusStop(params);
    });

});
