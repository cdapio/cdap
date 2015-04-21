angular.module(PKG.name + '.feature.admin')
  .controller('ConfigurationController', function ($scope, MyDataSource) {
    var dataSrc = new MyDataSource($scope);

    $scope.config = [];

    dataSrc.request({
      _cdapPath: '/config/cdap'
    })
    .then(function (res) {

      $scope.config = res;

    });

  });
