angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceDetailController', function(MyDataSource, $state, $scope) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' +
            $state.params.appId +
            '/mapreduce/' +
            $state.params.programId;
    $scope.runs = null;

    dataSrc.poll({
      _cdapNsPath: basePath + '/runs'
    }, function(res) {
        $scope.runs = res;
        $scope.activeRuns = (res.map(function(r) {
          return (r.status === 'RUNNING'? 1: 0);
        }) || [])
          .reduce(function(prev, curr) {
            return prev + curr;
          });
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
