angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsDetailRunStatusController', function($state, $scope, MyDataSource) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/workflows/' + $state.params.programId;
    $scope.data = {};
    $scope.status = null;
    $scope.duration = null;
    $scope.startTime = null;
    dataSrc.request({
      _cdapNsPath: basePath
    })
      .then(function(res) {
        console.log("Workflow: ", res);
      });


    dataSrc.poll({
      _cdapNsPath: basePath + '/runs'
    }, function(res) {
        var startTime, endTime, duration;
        angular.forEach(res, function(run) {
          startTime = run.start * 1000;
          endTime = run.end * 1000;
          if (run.runid === $state.params.runId) {
            $scope.status = run.status;
            duration = ((startTime? endTime - startTime: Date.now() - startTime) / 1000) || 'N/A';
            if (duration < 60000) {
              $scope.duration = duration + ' seconds';
            } else if (duration > 60000 && duration < 3600000) {
              $scope.duration = duration + ' minutes';
            } else {
              // Assuming that none of the 'scheduled' workflow programs
              // are going to run for more than an hour. If yes then he is a
              // worse developer than me.
              $socpe.duration = 'N/A';
            }
            $scope.startTime = new Date(startTime);
          }
        });
      });

  });
