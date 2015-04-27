angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceRunDetailLogsController', function($scope, MyDataSource, $state) {
    var dataSrc = new MyDataSource($scope),
      basePath = '/apps/' + $state.params.appId + '/mapreduce/' + $state.params.programId + '/runs/' + $scope.current;

    $scope.logs = [];

    dataSrc.poll({
      _cdapNsPath: basePath + '/logs/next?maxSize=50'
    }, function(res) {
      $scope.logs = res;
    });
  });
