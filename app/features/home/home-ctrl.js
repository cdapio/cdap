/**
 * HomeCtrl
 */

angular.module(PKG.name+'.feature.home').controller('HomeCtrl',
function ($scope, $alert, MyDataSource, myNamespace) {
  $scope.apps = null;
  $scope.hideWelcomeMessage = false;

  var dataSrc = new MyDataSource($scope);

  dataSrc.request({
    _cdapPath: '/apps/',
    method: 'GET'
  }, function(res) {
    $scope.apps = res;
    if (angular.isArray($scope.apps) && $scope.apps.length) {
      $scope.dataAppsTemplate = 'assets/features/home/templates/data-apps-section.html';
    } else {
      $scope.dataAppsTemplate = 'assets/features/home/templates/data-apps-default-view.html';
    }
    console.log('Apps: ', $scope.apps);
  });

});
