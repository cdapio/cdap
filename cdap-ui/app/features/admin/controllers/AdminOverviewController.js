angular.module(PKG.name + '.feature.admin')
  .controller('AdminOverviewController', function ($scope, $state, myNamespace, MyDataSource, myLocalStorage, MY_CONFIG, myStreamApi, myDatasetApi) {
    var dataSrc = new MyDataSource($scope),
        PREFKEY = 'feature.admin.overview.welcomeIsHidden';

    myLocalStorage.get(PREFKEY)
      .then(function (v) {
        $scope.welcomeIsHidden = v;
      });

    $scope.hideWelcome = function () {
      myLocalStorage.set(PREFKEY, true);
      $scope.welcomeIsHidden = true;
    };

    $scope.isEnterprise = MY_CONFIG.isEnterprise;

    // TODO: add dataset and stream counts per namespace
    myNamespace.getList()
      .then(function (list) {
        $scope.nsList = list;
        $scope.nsList.forEach(function (namespace) {
          getApps(namespace)
            .then(function (apps) {
              namespace.appsCount = apps.length;
            });
          getDatasets(namespace)
            .then(function (data) {
              namespace.datasetsCount = data.length;
            });

          getStreams(namespace)
            .then(function (streams) {
              namespace.streamsCount = streams.length;
            });
        });
      });

    function getApps (namespace) {
      return dataSrc.request({
        _cdapPath: '/namespaces/' + namespace.name + '/apps'
      });
    }

    function getDatasets (namespace) {
      var params = {
        namespace: namespace.name,
        scope: $scope
      };
      return myDatasetApi.list(params).$promise;
    }

    function getStreams (namespace) {
      var params = {
        namespace: namespace.name,
        scope: $scope
      };
      return myStreamApi.list(params).$promise;
    }
  });
