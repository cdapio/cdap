angular.module(PKG.name + '.feature.streams')
  .controller('CdapStreamsListController', function($scope, MyDataSource) {
    var dataSrc = new MyDataSource($scope);

    dataSrc.request({
      _cdapNsPath: '/streams'
    })
      .then(function(res) {
        $scope.streams = res;
      });
  });
