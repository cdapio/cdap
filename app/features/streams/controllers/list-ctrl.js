angular.module(PKG.name + '.feature.streams')
  .controller('CdapStreamsListController', function($scope, MyDataSource) {
    var dataSrc = new MyDataSource($scope);

    dataSrc.request({
      _cdapPathV2: '/streams'
    })
      .then(function(res) {
        $scope.streams = res;
      })
  })
