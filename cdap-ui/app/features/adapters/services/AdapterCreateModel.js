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

    Model.prototype.resetPlugins = function resetPlugins() {
      this.source = angular.copy(defaultSource);
      this.transforms = angular.copy(defaultTransforms);
      this.sink = angular.copy(defaultSink);
      this.schedule = {
        cron: ''
      };
    };

    Model.prototype.setSource = function setSource(source) {
      this.source = source;
    };

    Model.prototype.setTransform = function setTransform(transform) {
      if (this.transforms[0].placeHolder) {
        this.transforms[0] = transform;
      } else {
        this.transforms.push(transform);
      }
    };

    Model.prototype.setSink = function setSink(sink) {
      this.sink = sink;
    };

    return Model;

  });
