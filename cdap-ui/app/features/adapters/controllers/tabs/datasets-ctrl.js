angular.module(PKG.name + '.feature.adapters')
  .controller('AdapterDatasetsController', function($scope, $state, myAdapterApi) {
    $scope.dataList = [];

    var params = {
      namespace: $state.params.namespace,
      adapter: $state.params.adapterId,
      scope: $scope
    };

    myAdapterApi.datasets(params)
      .$promise
      .then(function(res) {
        angular.forEach(res, function(dataset) {
          dataset.name = dataset.instanceId;
          dataset.type = 'Dataset';
        });
        $scope.dataList = $scope.dataList.concat(res);
      });

    myAdapterApi.streams(params)
      .$promise
      .then(function(res) {
        angular.forEach(res, function(stream) {
          stream.name = stream.streamName;
          stream.type = 'Stream';
        });
        $scope.dataList = $scope.dataList.concat(res);
      });
  });
