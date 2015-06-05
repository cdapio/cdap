angular.module(PKG.name + '.feature.adapters')
  // TODO: We should use rAdapterDetail here since this data is already resolved at adapter.detail state
  .controller('AdapterRunDetailStatusController', function($scope, MyDataSource, $state) {
    var dataSrc = new MyDataSource($scope);

    $scope.transforms = [{
      name: '',
      properties: {},
      type: ''
    }];
    $scope.source = {
      name: '',
      properties: {},
      type: ''
    };
    $scope.sink = {
      name: '',
      properties: {},
      type: ''
    };

    dataSrc.request({
      _cdapNsPath: '/adapters/' + $state.params.adapterId
    })
      .then(function(res) {
        $scope.source = res.config.source;
        $scope.sink = res.config.sink;
        $scope.transforms = res.config.transforms || [];
      });
});
