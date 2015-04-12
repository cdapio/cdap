angular.module(PKG.name + '.feature.admin')
  .controller('ConfigurationController', function ($scope, MyDataSource) {
    var dataSrc = new MyDataSource($scope);

    $scope.config = [];

    dataSrc.request({
      _cdapPath: '/config/cdap?format=json'
    })
    .then(function (res) {
      angular.forEach(res, function(v, k) {
        $scope.config.push({
          key: k,
          value: v
        });
      });
    });

  });
