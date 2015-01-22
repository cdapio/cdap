angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetsListController', function($scope, MyDataSource) {
    var datasrc = new MyDataSource($scope);
    $scope.datasets = datasrc.request({
      _cdapPathV2: '/data/datasets'
    });
  });
