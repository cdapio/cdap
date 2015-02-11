angular.module(PKG.name + '.feature.admin')
  .controller('AdminOverviewController', function ($scope, $state, myNamespace, MyDataSource) {
    var dataSrc = new MyDataSource($scope);
    $scope.hideWelcomeMessage = false;

    // TODO: add dataset and stream counts per namespace
    myNamespace.getList()
      .then(function(list) {
        $scope.nsList = list;
        $scope.nsList.forEach(function(namespace) {
          getApps(namespace)
            .then(function(apps) {
              namespace.appsCount = apps.length;
            });
        });
      });

    function getApps(namespace) {
      return dataSrc.request({
        _cdapPath: '/namespaces/' + namespace.id + '/apps'
      })
    }
  });
