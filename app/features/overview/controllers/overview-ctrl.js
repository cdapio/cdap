/**
 * OverviewCtrl
 */

angular.module(PKG.name+'.feature.overview').controller('OverviewCtrl',
function ($scope, MyDataSource, $state, myLocalStorage, MY_CONFIG) {

  if(!$state.params.namespace) {
    // the controller for "ns" state should handle the case of
    // an empty namespace. but this nested state controller will
    // still be instantiated. avoid making useless api calls.
    return;
  }

  $scope.apps = [];
  $scope.datasets = [];
  $scope.hideWelcomeMessage = false;

  var dataSrc = new MyDataSource($scope),
      partialPath = '/assets/features/overview/templates/',
      PREFKEY = 'feature.overview.welcomeIsHidden';

  myLocalStorage.get(PREFKEY)
    .then(function (v) {
      $scope.welcomeIsHidden = v;
    });

  $scope.hideWelcome = function () {
    myLocalStorage.set(PREFKEY, true);
    $scope.welcomeIsHidden = true;
  };

  $scope.isEnterprise = MY_CONFIG.isEnterprise;

  dataSrc.request({
    _cdapNsPath: '/apps'
  })
    .then(function(res) {
      $scope.apps = res;
      var isValidArray = angular.isArray($scope.apps) && $scope.apps.length;
      $scope.appsTemplate = partialPath +
        (isValidArray ? 'apps-section.html': 'apps-empty-section.html');
      console.log('Apps: ', $scope.apps);
    });

  dataSrc.request({
    _cdapPathV2: '/data/datasets'
  })
    .then(function(res) {
      $scope.datasets = res;
      var isValidArray = angular.isArray($scope.datasets) && $scope.datasets.length;
      $scope.dataTemplate = partialPath +
        (isValidArray ? 'data-section.html': 'data-empty-section.html');
      console.log('Datasets: ', $scope.datasets);
    });

  dataSrc.request({
    _cdapPathV2: '/streams'
  }, function(res) {
    if (angular.isArray(res) && res.length) {
      $scope.streams = res;
    }
  });

});
