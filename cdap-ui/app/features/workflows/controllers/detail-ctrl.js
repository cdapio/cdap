angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsDetailController', function($scope, $state, $timeout, MyDataSource, WorkflowsFactory) {
    var dataSrc = new MyDataSource($scope),
        params = {
          appId: $state.params.appId,
          workflowId: $state.params.programId
        };

    var wf = WorkflowsFactory;

    wf.runs(params, function(res) {

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

    wf.getStatus(params, function(res) {
      $scope.status = res.status;
    });

    $scope.toggleFlow = function(action) {
      $scope.status = action;
      wf[action](params);
    };

});
