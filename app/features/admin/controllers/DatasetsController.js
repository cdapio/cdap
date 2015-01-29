angular.module(PKG.name + '.feature.admin')
  .controller('DatasetsController', function ($scope) {
    $scope.datasets = [{
      name: 'STU',
      type: 'Stream',
      description: 'little miss muffet',
      apps: '2'
    }];
  });
