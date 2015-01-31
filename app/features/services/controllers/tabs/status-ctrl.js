angular.module(PKG.name + '.feature.services')
  .controller('CdapServicesDetailStatusController', function($state, $scope, MyDataSource) {
    var dataSrc = new MyDataSource($scope),
        path = '/apps/' +
          $state.params.appId + '/services/' +
          $state.params.programId;

    dataSrc.request({
      _cdapNsPath: path
    })
      .then(function(res) {
        console.log("services: ", res);
      });
  });
