angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetsListController', function($scope, MyDataSource) {
    var datasrc = new MyDataSource($scope);
    datasrc.request({
      _cdapNsPath: '/data/datasets'
    })
      .then(function(datasets) {
        $scope.datasets = datasets;
      });
  });
