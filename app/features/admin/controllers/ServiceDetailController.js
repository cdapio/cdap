angular.module(PKG.name + '.feature.admin')
  .controller('AdminServiceDetailController', function ($scope, $state) {
    console.log($state.params.serviceName);
    $scope.service = {
        name: $state.params.serviceName
    };
  });
