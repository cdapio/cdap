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

  $scope.dataList = [];
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
  $scope.systemStatus = '';
  dataSrc.poll({
    _cdapPath: '/system/services/status'
  }, function(res) {
    var serviceStatuses = Object.keys(res).map(function(value, i) {
      return res[value];
    });
    if (serviceStatuses.indexOf('NOT OK') > -1) {
      $scope.systemStatus = 'yellow';
    }
    if (serviceStatuses.indexOf('OK') === -1) {
      $scope.systemStatus = 'red';
    }
    if (serviceStatuses.indexOf('NOT OK') === -1) {
      $scope.systemStatus = 'green';
    }
  });

  dataSrc.request({
    _cdapNsPath: '/apps'
  })
    .then(function(res) {
      $scope.apps = res;
    });

  dataSrc.request({
    _cdapNsPath: '/data/datasets'
  })
    .then(function(res) {
      $scope.dataList = $scope.dataList.concat(res);
    });

  dataSrc.request({
    _cdapNsPath: '/streams'
  }, function(res) {
    if (angular.isArray(res) && res.length) {
      $scope.dataList = $scope.dataList.concat(res);
    }
  });

});
