angular.module(PKG.name + '.feature.applications')
  .controller('ApplicationListController', function($scope, MyDataSource) {

    var data = new MyDataSource($scope);

    data.request({
      _cdapNsPath: '/apps/',
      method: 'GET',
    }, function(res) {
      $scope.apps = res;
    });

  });
