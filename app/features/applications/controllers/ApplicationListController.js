angular.module(PKG.name + '.feature.applications')
  .controller('ApplicationListController', function($scope, MyDataSource) {

    var data = new MyDataSource($scope);

    data.fetch({
      config: {
        method: 'GET',
        path: '/apps/'
      }
    }, function(res) {
      $scope.apps = res;
    });

  });
