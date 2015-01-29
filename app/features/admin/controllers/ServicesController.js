angular.module(PKG.name + '.feature.admin')
  .controller('AdminServicesController', function ($scope) {
    $scope.services = [{
      name: 'appfabric',
      status: 'active',
      provisioned: '2',
      requested: '2'
    }];
  });
