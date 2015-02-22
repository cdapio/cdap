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
        href: '#'
      }
    ];

  });
