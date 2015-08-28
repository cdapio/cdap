angular.module(PKG.name + '.feature.datasets')
  .controller('DatasetExploreController',
    function($state) {

      var datasetId = $state.params.datasetId;
      datasetId = datasetId.replace(/[\.\-]/g, '_');
      this.name = datasetId;

    }
  );
