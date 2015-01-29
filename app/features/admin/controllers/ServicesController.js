angular.module(PKG.name + '.feature.admin')
  .controller('ServicesController', function ($scope) {
    $scope.services = [{
      name: 'appfabric',
      status: 'active',
      provisioned: '2',
      requested: '2'
    }];
  });
