angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorDetailDatasetsController', function(DetailRunsStore) {
    this.setDefaults = function() {
      this.state = {
        data: DetailRunsStore.getDatasets().concat(DetailRunsStore.getStreams())
      };
    };
    this.setDefaults();

    this.setState = function() {
      this.state.datasets =  DetailRunsStore.getDatasets().concat(DetailRunsStore.getStreams());
    };
    
    DetailRunsStore.registerOnChangeListener(this.setState.bind(this));
  });
