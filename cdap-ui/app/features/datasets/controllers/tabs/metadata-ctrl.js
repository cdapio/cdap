angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetMetadataController',
    function($scope, $state, myExploreApi) {

      var params = {
        namespace: $state.params.namespace,
        table: 'dataset_' + $state.params.datasetId,
        scope: $scope
      };

      myExploreApi.getInfo(params)
        .$promise
        .then(function (res) {
          $scope.metadata = res;
        });

    }
  );
