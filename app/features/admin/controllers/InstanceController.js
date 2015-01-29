angular.module(PKG.name + '.feature.admin')
  .controller('InstanceController', function ($scope) {
    $scope.instance = [{
      id: '1984',
      key: 'string',
      value: 'abcdefg'
    }];
  });
