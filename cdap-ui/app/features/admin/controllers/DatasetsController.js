angular.module(PKG.name + '.feature.admin')
  .controller('AdminDatasetsController', function ($scope, MyDataSource, $stateParams) {

    var dataSrc = new MyDataSource($scope);

    $scope.datasets = [];
    $scope.streams = [];


    dataSrc.request({
      _cdapPath: '/namespaces/' + $stateParams.nsadmin + '/data/datasets'
    })
      .then(function(res) {
        $scope.datasets = res;
      });

    dataSrc.request({
      _cdapPath: '/namespaces/' + $stateParams.nsadmin + '/streams'
    }, function(res) {
      angular.forEach(res, function(r) {
        r.type = 'Stream';
      });

      $scope.streams = res;
    });
  });
