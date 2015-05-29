angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetsDetailController', function($scope, $state, MyDataSource, $alert, $filter, myDatasetApi, myExploreApi) {
    var filterFilter = $filter('filter');

    $scope.explorable = null;
    var params = {
      namespace: $state.params.namespace,
      scope: $scope
    };

    // Checking whether dataset is explorable
    myExploreApi.list(params)
      .$promise
      .then(function (res) {
        var match = filterFilter(res, $state.params.datasetId);

        if (match.length === 0) {
          $scope.explorable = false;
        } else {
          $scope.explorable = true;
        }
      });


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
