angular.module(PKG.name + '.feature.services')
  .controller('CdapServicesDetailStatusController', function($state, $scope, MyDataSource) {
    var dataSrc = new MyDataSource($scope),
        path = '/apps/' +
          $state.params.appId + '/services/' +
          $state.params.programId;

    $scope.endPoints = [];

    dataSrc.request({
      _cdapNsPath: path
    })
      .then(function(res) {
        angular.forEach(res.handlers, function(value, key) {
          $scope.endPoints = $scope.endPoints.concat(value.endpoints);
        });
      });
  });
