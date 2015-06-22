angular.module(PKG.name + '.feature.datasets')
  .controller('DatasetMetadataController',
    function($scope, $state, myExploreApi) {

      var datasetId = $state.params.datasetId;
      datasetId = datasetId.replace(/[\.\-]/g, '_');

      var params = {
        namespace: $state.params.namespace,
        table: 'dataset_' + datasetId,
        scope: $scope
      };
      this.metadata = {};
      myExploreApi.getInfo(params)
        .$promise
        .then(function (res) {
          this.metadata = res;
        }.bind(this));

    }
  );
