angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetExploreController',
    function($scope, $state, EventPipe) {


      $scope.activePanel = [0];
      $scope.name = $state.params.datasetId;

      EventPipe.on('explore.newQuery', function() {
        $scope.activePanel = [0,1];
      });

    }
  );
