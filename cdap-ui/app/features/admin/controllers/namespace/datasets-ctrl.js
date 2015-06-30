angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceDatasetsController', function ($scope, $stateParams, myStreamApi, myDatasetApi) {

    $scope.dataList = [];
    $scope.currentPage = 1;

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
