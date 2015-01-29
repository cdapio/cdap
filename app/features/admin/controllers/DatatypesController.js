angular.module(PKG.name + '.feature.admin')
  .controller('AdminDatatypesController', function ($scope) {
    $scope.datatypes = [{
      name: 'counter',
      className: 'core',
      description: 'goldilocks and the 3 bears',
      instances: '2',
      apps: '5'
    }];
  });
