angular.module(PKG.name + '.feature.admin')
  .controller('AdminDatasetsController', function ($scope, MyDataSource, $stateParams) {

    var dataSrc = new MyDataSource($scope);
    $scope.dataList = [];

    dataSrc.request({
      _cdapPath: '/namespaces/' + $stateParams.nsadmin + '/data/datasets'
    })
      .then(function(res) {
        $scope.dataList = $scope.dataList.concat(res);
      });

    dataSrc.request({
      _cdapPath: '/namespaces/' + $stateParams.nsadmin + '/streams'
    }, function(res) {
      angular.forEach(res, function(r) {
        r.type = 'Stream';
      });
      $scope.dataList = $scope.dataList.concat(res);
    });
  });
