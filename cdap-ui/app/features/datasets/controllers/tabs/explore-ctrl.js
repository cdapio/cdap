angular.module(PKG.name + '.feature.datasets')
  .controller('DatasetExploreController',
    function($state, EventPipe) {

      this.activePanel = [0];

      var datasetId = $state.params.datasetId;
      datasetId = datasetId.replace(/[\.\-]/g, '_');
      this.name = datasetId;

      EventPipe.on('explore.newQuery', function() {
        this.activePanel = [0,1];
      }.bind(this));

    }
  );
