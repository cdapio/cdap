angular.module(PKG.name + '.feature.cdap-app')
  .controller('CdapAppListController', function CdapAppList($scope, MyDataSource) {

    var data = new MyDataSource($scope);

    data.request({
      _cdapNsPath: '/apps/',
      method: 'GET',
    }, function(res) {
      $scope.apps = res;
    });

  });
