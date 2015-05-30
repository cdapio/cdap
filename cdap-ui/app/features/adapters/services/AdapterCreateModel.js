angular.module(PKG.name + '.feature.adapters')
  .factory('AdapterCreateModel', function() {
    var defaultSource = {
      name: 'Add a source',
      properties: {},
      placeHolder: true
    };
    var defaultSink = {
      name: 'Add a sink',
      properties: {},
      placeHolder: true
    };
    var defaultTransforms = [{
      name: 'Add a transform',
      properties: {},
      placeHolder: true
    }];
    var defaultMetadata = {
        name: '',
        description: '',
        type: 'ETLBatch'
    };

    function Model () {
      this.metadata = defaultMetadata;
      this.resetPlugins();
    }

    Model.prototype.resetPlugins = function() {
      this.source = angular.copy(defaultSource);
      this.transforms = angular.copy(defaultTransforms);
      this.sink = angular.copy(defaultSink);
      this.schedule = {
        cron: ''
      };
    }

    return Model;

  });
