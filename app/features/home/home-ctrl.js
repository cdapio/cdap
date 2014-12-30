/**
 * HomeCtrl
 */

angular.module(PKG.name+'.feature.home').controller('HomeCtrl',
function ($location, $state, $scope, $alert, MyDataSource, namespaceList) {
  $scope.apps = null;
  $scope.hideWelcomeMessage = false;
  var dataSrc = new MyDataSource($scope);
  if (!$state.params.namespaceId) {
    $state.params.namespaceId = namespaceList[0].displayName;
    $location.path($location.url() + $state.params.namespaceId);
  }
  dataSrc.request({
    _cdapNsPath: '/apps/',
    method: 'GET'
  }, function(res) {
    debugger;
    $scope.apps = res;
    if (angular.isArray($scope.apps) && $scope.apps.length) {
      $scope.dataAppsTemplate = 'assets/features/home/templates/data-apps-section.html';
    } else {
      $scope.dataAppsTemplate = 'assets/features/home/templates/data-apps-default-view.html';
    }
    console.log('Apps: ', $scope.apps);
  });

});
