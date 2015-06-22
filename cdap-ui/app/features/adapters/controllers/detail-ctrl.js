angular.module(PKG.name + '.feature.adapters')
  .controller('AdpaterDetailController', function($scope, rAdapterDetail) {
    $scope.template = rAdapterDetail.template;
  });
