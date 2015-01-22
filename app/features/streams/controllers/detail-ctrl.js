angular.module(PKG.name + '.feature.streams')
  .controller('CdapStreamsDetailController', function($scope, MyDataSource, $stateParams) {
    var dataSrc = new MyDataSource($scope);

    dataSrc.request({
      _cdapPathV2: '/streams/' + $stateParams.streamId
    })
      .then(function(streams) {
        debugger;
        $scope.stream = streams;
      });
  });
