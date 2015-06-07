angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetExploreController',
    function($scope, $state, EventPipe) {

      $scope.activePanel = [0];

      var datasetId = $state.params.datasetId;
      datasetId = datasetId.replace(/[\.\-]/g, '_');
      $scope.name = datasetId;

      EventPipe.on('explore.newQuery', function() {
        $scope.activePanel = [0,1];
      });

    }
  );
