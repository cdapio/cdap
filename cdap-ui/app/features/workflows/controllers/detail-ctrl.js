angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsDetailController', function($scope, $state, $timeout, MyDataSource) {
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
      dataSrc.request({
        _cdapNsPath: basePath + '/start',
        method: 'POST'
      })
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

});
