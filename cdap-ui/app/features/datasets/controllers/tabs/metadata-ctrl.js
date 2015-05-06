angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetMetadataController',
    function($scope, MyDataSource, $state) {

      var dataSrc = new MyDataSource($scope);

      dataSrc.request({
        _cdapNsPath: '/data/explore/tables/dataset_' + $state.params.datasetId + '/info'
      })
      .then(function(res) {
        $scope.metadata = res;
      });

    }
  );
