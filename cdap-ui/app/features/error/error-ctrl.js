angular.module(PKG.name + '.feature.error')
  .controller('errorPopoverController', function($scope, myAlert) {
    $scope.alerts = myAlert.getAlerts();

    $scope.clear = function () {
      myAlert.clear();
      $scope.alerts = myAlert.getAlerts();
    };

  });
