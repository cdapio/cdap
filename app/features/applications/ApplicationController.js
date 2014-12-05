angular.module(PKG.name + '.feature.applications')
  .controller('ApplicationController', function($scope, MyDataSource) {

    var data = new MyDataSource($scope);

    data.fetch({
      _cdap: "GET /apps/"
    }, function(res) {
      $scope.apps = res;
    });

  });
