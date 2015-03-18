angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsDetailController', function($scope, $state, myWorkFlowApi) {
    var params = {
      appId: $state.params.appId,
      workflowId: $state.params.programId,
      scope: $scope
    };


    var prom = myWorkFlowApi.runs(angular.extend({status: 'running'}, params))
    prom.$promise.then(function(res) {
      $scope.activeRuns = res.length || 0;
    });

    $scope.activeRuns = 0;
    $scope.runs = null;

    myWorkFlowApi.status(params)
      .$promise.then(function(res) {
        $scope.status = res.status;
      });

    $scope.toggleFlow = function(action) {
      $scope.status = action;
      myWorkFlowApi[action](params, {});
    };

    $scope.$on('$destroy', function() {
      myWorkFlowApi.runsStop(params);
      myWorkFlowApi.statusStop(params);
    });

});
