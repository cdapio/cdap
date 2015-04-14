angular.module(PKG.name + '.feature.etlapps')
  .controller('EtlAppsListController', function($scope, MyDataSource) {
    var dataSrc = new MyDataSource($scope);
    $scope.etlapps  = [];
    dataSrc.request({
      _cdapNsPath: '/adapters?template=etl.realtime'
    })
      .then(function(res) {
        $scope.etlapps = res;
      });
  });
