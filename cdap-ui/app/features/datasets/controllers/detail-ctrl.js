angular.module(PKG.name + '.feature.datasets')
  .controller('DatasetsDetailController', function($scope, $state, MyDataSource, $alert, $filter, myDatasetApi, explorableDatasets) {
    var filterFilter = $filter('filter');

    var params = {
      namespace: $state.params.namespace,
      scope: $scope
    };

    this.explorable = explorableDatasets;

    this.truncate = function() {
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
