angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetsDetailController', function($scope, $state, MyDataSource, $alert, $filter, myDatasetApi) {
    var filterFilter = $filter('filter');

    var dataSrc = new MyDataSource($scope);
    $scope.explorable = null;

    dataSrc.request({
      _cdapNsPath: '/data/explore/tables'
    })
    .then(function(res) {
      var match = filterFilter(res, $state.params.datasetId);

      if (match.length === 0) {
        $scope.explorable = false;
      } else {
        $scope.explorable = true;
      }
    });

    $scope.truncate = function() {
      var params = {
        namespace: $state.params.namespace,
        datasetId: $state.params.datasetId,
        scope: $scope
      };
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
