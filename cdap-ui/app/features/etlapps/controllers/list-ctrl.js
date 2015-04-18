angular.module(PKG.name + '.feature.etlapps')
  .controller('EtlAppsListController', function($scope, MyDataSource) {
    var dataSrc = new MyDataSource($scope);
    $scope.etlapps  = [];
    dataSrc.request({
      _cdapNsPath: '/adapters?template=etl.realtime'
    })
      .then(function(res) {
        $scope.etlapps = res;
        angular.forEach($scope.etlapps, function(app) {
          app.status =  (Date.now()/2)? 'Running': 'Stopped';
          app.description = 'Something something dark.Something Something something dark';
        });
      });
  });
