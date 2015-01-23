angular.module(PKG.name + '.feature.streams')
  .controller('CdapStreamsDetailController', function($scope, MyDataSource, $stateParams) {
    var dataSrc = new MyDataSource($scope);
    $scope.dropdown = [
      {
        text: 'Action',
        href: '#'
      },
      {
        text: 'Action1',
        href: "#"
      }
    ];
    
    dataSrc.request({
      _cdapPathV2: '/streams/' + $stateParams.streamId
    })
      .then(function(streams) {
        debugger;
        $scope.stream = streams;
      });
  });
