angular.module(PKG.name + '.feature.streams')
  .controller('CdapStreamDetailController', function($scope, MyDataSource, $state) {
    var dataSrc = new MyDataSource($scope);
    // TODO: Implement Actions that can be performed in a stream
    // This will be towards the Zero-app experience.
    $scope.dropdown = [
      {
        text: 'Action',
        href: '#'
      },
      {
        text: 'Action1',
        href: '#'
      }
    ];

  });
