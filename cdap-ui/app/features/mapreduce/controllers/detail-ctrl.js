angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceDetailController', function(MyDataSource, $state, $scope) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' +
            $state.params.appId +
            '/mapreduce/' +
            $state.params.programId;
    $scope.runs = null;
    $scope.activeRuns = 0;
    dataSrc.poll({
      _cdapNsPath: basePath + '/runs'
    }, function(res) {
        $scope.runs = res;
        var count = 0;
        angular.forEach(res, function(value) {
          if (value.status === 'RUNNING') {
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

    $scope.toggleFlow = function(action) {
      $scope.status = action;
      dataSrc.request({
        method: 'POST',
        _cdapNsPath: basePath + '/' + action
      });
    };
  });
