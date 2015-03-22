angular.module(PKG.name + '.feature.error')
  .controller('errorPopoverController', function($scope, myAlert) {
    $scope.alerts = myAlert.list;
    console.log($scope.alerts);

  });