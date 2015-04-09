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
  $scope.streams = [];
  $scope.hideWelcomeMessage = false;
  //$scope.mockData = [];
  $scope.chartHistory = [
    {
      label: 'mockData',
      values: loopGraph()
    }
  ];
  $scope.appHistory = [
    {
      label: 'mockData',
      values: loopGraph()
    }
  ];
  function loopGraph() {
    var arr = [];
    for (var i = 0; i < 100; i++) {
      arr.push({
        time: i, y: Math.floor(Math.random() * 500 + 1)
      });
    }

    return arr;
  }

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
      $scope.datasets = res;
    });

  dataSrc.request({
    _cdapNsPath: '/streams'
  }, function(res) {
    if (angular.isArray(res) && res.length) {
      $scope.streams = res;
    }
  });

});
