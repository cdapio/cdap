angular.module(PKG.name + '.feature.adapters')
  .controller('AdapterRunDetailStatusController', function($scope, MyDataSource, $state) {
    var dataSrc = new MyDataSource($scope);

    $scope.transforms = [{
      name: '',
      properties: {}
    }];
    $scope.source = {
      name: '',
      properties: {}
    };
    $scope.sink = {
      name: '',
      properties: {}
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
