angular.module(PKG.name + '.feature.applications')
  .controller('ApplicationListController', function($scope, MyDataSource) {

    var data = new MyDataSource($scope);

    data.request({
      _cdapPath: '/apps/',
      method: 'GET',
    }, function(res) {
      $scope.apps = res;
    });

  });
