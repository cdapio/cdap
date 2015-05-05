angular.module(PKG.name + '.feature.adapters')
  .controller('AdapterDatasetsController', function($scope, MyDataSource, $state) {
    var dataSrc = new MyDataSource($scope);
    $scope.dataList = [];
    dataSrc.request({
      _cdapNsPath: '/adapters/' + $state.params.adapterId + '/datasets'
    })
      .then(function(res) {
        angular.forEach(res, function(dataset) {
          dataset.name = dataset.instanceId;
          dataset.type = 'Dataset';
        });
        $scope.dataList = $scope.dataList.concat(res);
      });

    dataSrc.request({
      _cdapNsPath: '/adapters/' + $state.params.adapterId + '/streams'
    })
      .then(function(res) {
        angular.forEach(res, function(stream) {
          stream.name = stream.streamName;
          stream.type = 'Stream';
        });
        $scope.dataList = $scope.dataList.concat(res);
      });
  });
