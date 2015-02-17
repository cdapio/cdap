angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsDetailRunStatusController', function($state, $scope, MyDataSource, amMoment) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/workflows/' + $state.params.programId;
    $scope.moment = amMoment;
    $scope.moment.changeLocale('en');
    $scope.status = null;
    $scope.duration = null;
    $scope.startTime = null;
    dataSrc.request({
      _cdapNsPath: basePath
    })
      .then(function(res) {
        $scope.actions = res.actions;
      });


    $scope.goToDetailActionView = function(programId, programType) {
      // As of 2.7 only a mapreduce job is scheduled in a workflow.
      if (programType === 'MAPREDUCE') {
        $state.go('mapreduce.detail', {
          programId: programId
        });
      }
    };

    dataSrc.poll({
      _cdapNsPath: basePath + '/runs'
    }, function(res) {
        angular.forEach(res, function(run) {
          $scope.startTime = new Date(run.start * 1000);
          $scope.status = run.status;
          $scope.duration = (run.end ? (run.end * 1000) - $scope.startTime : 0);
          console.log("Duration:", $scope.duration);
        });
      });

  });
