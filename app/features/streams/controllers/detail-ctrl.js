angular.module(PKG.name + '.feature.streams')
  .controller('CdapStreamDetailController', function($scope, MyDataSource, $state) {
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
      _cdapPathV2: '/streams/' + $state.params.streamId
    })
      .then(function(streams) {
        $scope.stream = streams;
      });

    $scope.tabs = [
      'Status',
      'Explore',
      'Programs',
      'Inner',
      'Metadata',
      'Lineage',
      'Resources'
    ];
    $scope.tabsPartialPath = '/assets/features/streams/templates/tabs/';
    
  });
