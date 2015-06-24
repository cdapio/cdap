angular.module(PKG.name + '.feature.admin')
  .controller('SystemInstanceController', function ($scope) {
    $scope.instance = [{
      id: '1984',
      key: 'string',
      value: 'abcdefg'
    }];
  });
