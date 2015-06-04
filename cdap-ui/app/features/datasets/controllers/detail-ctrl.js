angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetsDetailController', function($scope, $state, MyDataSource, $alert, $filter, myDatasetApi, explorableDatasets) {
    var filterFilter = $filter('filter');

    $scope.explorable = null;
    var params = {
      namespace: $state.params.namespace,
      scope: $scope
    };

    $scope.explorable = explorableDatasets;

    $scope.truncate = function() {
      params.datasetId = $state.params.datasetId;
      myDatasetApi.truncate(params, {})
        .$promise
        .then(function () {
          $alert({
            content: 'Succesfully truncated ' + $state.params.datasetId + ' dataset',
            type: 'success'
          });
        });
    };

  });
