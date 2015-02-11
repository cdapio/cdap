angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceLogsController', function($scope, MyDataSource, $state) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/mapreduce/' + $state.params.programId;

    $scope.logs = [];

    dataSrc.poll({
      _cdapNsPath: basePath + '/logs/next?fromOffset=-1&maxSize=50'
    }, function(res) {
      $scope.logs = res;
    });

  });
