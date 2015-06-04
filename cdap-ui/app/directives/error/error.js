/**
 * myError
 */

angular.module(PKG.name+'.commons')

.controller('myErrorController', function($scope, myAlert) {
  $scope.alerts = myAlert.getAlerts();

  $scope.clear = function () {
    $scope.$hide();
    myAlert.clear();
    $scope.alerts = myAlert.getAlerts();
  };

  $scope.remove = function (item) {
    myAlert.remove(item);
    if (myAlert.count() === 0) {
      $scope.$hide();
    }
  };
})

.directive('myError',
function myErrorDirective () {
  return {
    restrict: 'E',
    templateUrl: 'error/error-template.html',
    controller: function($scope, myAlert) {
      $scope.emptyError = function() {
        return myAlert.isEmpty();
      };
      $scope.errorCount = myAlert.count;
    }
  };

});
