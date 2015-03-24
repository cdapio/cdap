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
})

.directive('myError',
function myErrorDirective (myAlert) {
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
