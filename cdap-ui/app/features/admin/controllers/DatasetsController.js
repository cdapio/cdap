angular.module(PKG.name + '.feature.admin')
  .controller('AdminDatasetsController', function ($scope, MyDataSource, $stateParams, myStreamApi) {

    var dataSrc = new MyDataSource($scope);
    $scope.dataList = [];

    dataSrc.request({
      _cdapPath: '/namespaces/' + $stateParams.nsadmin + '/data/datasets'
    })
      .then(function(res) {
        $scope.dataList = $scope.dataList.concat(res);
      });

    var streamParams = {
      namespace: $stateParams.nsadmin,
      scope: $scope
    };
    myStreamApi.list(streamParams)
      .$promise
      .then(function (res) {
        angular.forEach(res, function(r) {
          r.type = 'Stream';
        });
        $scope.dataList = $scope.dataList.concat(res);
      });

  });
