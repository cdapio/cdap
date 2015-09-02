angular.module(PKG.name + '.feature.adapters')
  .controller('AdpaterDetailController', function($scope, rAdapterDetail) {
    $scope.template = rAdapterDetail.template;
    $scope.description = rAdapterDetail.description;
    $scope.isScheduled = false;
    if (rAdapterDetail.schedule) {
      $scope.isScheduled = true;
    }
  });
