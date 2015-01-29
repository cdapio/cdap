angular.module(PKG.name + '.feature.admin')
  .controller('AdminDatasetsController', function ($scope) {
    $scope.datasets = [{
      name: 'STU',
      type: 'Stream',
      description: 'little miss muffet',
      apps: '2'
    }];
  });
