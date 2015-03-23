angular.module(PKG.name + '.feature.error')
  .controller('errorPopoverController', function($scope, myAlert) {
    $scope.alerts = myAlert.list;

    $scope.clear = function () {
      myAlert.clear();
      $scope.alerts = myAlert.list;
    };

  });