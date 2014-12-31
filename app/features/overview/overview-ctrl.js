/**
 * OverviewCtrl
 */

angular.module(PKG.name+'.feature.overview').controller('OverviewCtrl',
function ($scope, MyDataSource) {
  $scope.apps = null;
  $scope.hideWelcomeMessage = false;

  var dataSrc = new MyDataSource($scope);

  dataSrc.request({
    _cdapNsPath: '/apps',
    method: 'GET'
  }, function(res) {
    $scope.apps = res;

    var p = 'assets/features/overview/templates/';
    if (angular.isArray($scope.apps) && $scope.apps.length) {
      $scope.dataAppsTemplate = p + 'section.html';
    } else {
      $scope.dataAppsTemplate = p + 'empty.html';
    }
    console.log('Apps: ', $scope.apps);
  });

});
