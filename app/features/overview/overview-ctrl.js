/**
 * OverviewCtrl
 */

angular.module(PKG.name+'.feature.overview').controller('OverviewCtrl',
function ($scope, MyDataSource, $state, myNamespace) {

  if(!$state.params.namespace) {
    myNamespace.getList().then(function (list) {
      $state.go($state.current, {
        namespace: list[0].displayName
      }, {reload: true});
    });
    return;
  }


  $scope.apps = null;
  $scope.hideWelcomeMessage = false;

  var dataSrc = new MyDataSource($scope);

  dataSrc.request({
    _cdapNsPath: '/apps',
    method: 'GET'
  }, function(res) {
    $scope.apps = res;

    var p = '/assets/features/overview/templates/';
    if (angular.isArray($scope.apps) && $scope.apps.length) {
      $scope.dataAppsTemplate = p + 'data-apps-section.html';
    } else {
      $scope.dataAppsTemplate = p + 'empty.html';
    }
    console.log('Apps: ', $scope.apps);
  });

});
