angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsDetailController', function($scope, $state, $timeout, MyDataSource, myProgramPreferencesService, myRuntimeService) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' +
            $state.params.appId +
            '/workflows/' +
            $state.params.programId;

    $scope.activeRuns = 0;
    $scope.runs = null;
    dataSrc.poll({
      _cdapNsPath: basePath + '/runs'
    }, function(res) {
        $scope.runs = res;
        var count = 0;
        angular.forEach(res, function(runs) {
          if (runs.status === 'RUNNING') {
            count += 1;
          }
        });
        $scope.activeRuns = count;
      });

    dataSrc.poll({
      _cdapNsPath: basePath + '/status'
    }, function(res) {
      $scope.status = res.status;
    });

    $scope.start = function() {
      $scope.status = 'STARTING';
      var requestObj = {
        _cdapNsPath: basePath + '/start',
        method: 'POST'
      };

      if ($scope.runtimeArgs && Object.keys($scope.runtimeArgs).length > 0) {
        requestObj.body = $scope.runtimeArgs;
      }

      dataSrc.request(requestObj)
        .then(function() {
          $state.go('workflows.detail.runs', {}, {reload: true});
        });
    };
    $scope.stop = function() {
      $scope.status = 'STOPPING';
      dataSrc.request({
        _cdapNsPath: basePath + '/stop',
        method: 'POST'
      });
    };

    $scope.openPreferences = function() {
      myProgramPreferencesService.show('workflows');
    };

    $scope.openRuntime = function() {
      myRuntimeService.show().result.then(function(res) {
        $scope.runtimeArgs = res;
      });
    };

});
