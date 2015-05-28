angular.module(PKG.name + '.feature.admin')
  .controller('AdminDatasetsController', function ($scope, $stateParams, myStreamApi, myDatasetApi) {

    $scope.dataList = [];

    var params = {
      namespace: $stateParams.nsadmin,
      scope: $scope
    };

    myDatasetApi.list(params)
      .$promise
      .then(function (res) {
        $scope.dataList = $scope.dataList.concat(res);
      });

    myStreamApi.list(params)
      .$promise
      .then(function (res) {
        angular.forEach(res, function(r) {
          r.type = 'Stream';
        });
        $scope.dataList = $scope.dataList.concat(res);
      });

  });
