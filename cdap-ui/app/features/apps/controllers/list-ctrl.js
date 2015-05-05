angular.module(PKG.name + '.feature.apps')
  .controller('CdapAppListController', function CdapAppList($timeout, $scope, MyDataSource, myAppUploader, $alert, $state, MyOrderings) {
    $scope.MyOrderings = MyOrderings;
    var data = new MyDataSource($scope);

    data.request({
      _cdapNsPath: '/apps/'
    })
      .then(function(apps) {
        $scope.apps = apps;
      });
    $scope.onFileSelected = myAppUploader.upload;

  });
