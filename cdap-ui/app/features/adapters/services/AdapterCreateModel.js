angular.module(PKG.name + '.feature.adapters')
  .factory('AdapterCreateModel', function(AdapterApiFactory, $state, $timeout, $q, mySettings) {
    var defaultSource = {
      name: 'Add a source',
      properties: {},
      valid: true,
      placeHolder: true
    };
    var defaultSink = {
      name: 'Add a sink',
      properties: {},
      valid: true,
      placeHolder: true
    };
    var defaultTransforms = [{
      name: 'Add a transform',
      properties: {},
      valid: true,
      placeHolder: true
    }];

    var defaultSchedule = {
      cron: '* * * * *'
    };

    function Model () {
      var defaultMetadata = {
          name: '',
          description: '',
          type: 'ETLBatch'
      };
      this.metadata = defaultMetadata;
      this.resetPlugins();
    }

    Model.prototype.resetPlugins = function resetPlugins() {
      this.source = angular.copy(defaultSource);
      this.transforms = angular.copy(defaultTransforms);
      this.sink = angular.copy(defaultSink);
      this.schedule = angular.copy(defaultSchedule);
    };

    Model.prototype.setMetadata = function (metadata) {
      // FIXME: There is a timing issue when editing a draft.
      this.metadata.type = metadata.type;
      this.metadata.name = metadata.name;
      this.metadata.description = metadata.description;
    };

    Model.prototype.setSource = function setSource(source) {
      this.source = source;
      this.source.valid = true;
    };

    Model.prototype.setTransform = function setTransform(transform) {
      if (!transform || !transform.length) {
        return;
      }
      transform.forEach(function(t) {
        t.valid = true;
      });
      if (this.transforms[0].placeHolder) {
        this.transforms = transform.slice();
      } else {
        this.transforms = this.transforms.concat(transform);
      }
    };

    Model.prototype.setSink = function setSink(sink) {
      this.sink = sink;
      this.sink.valid = true;
    };

    Model.prototype.setSchedule = function setSchedule(schedule) {
      this.schedule = schedule || defaultSchedule;
    };

    Model.prototype.saveAsDraft = function saveAsDraft() {
      var defer = $q.defer();
      if (!this.metadata.name.length) {
        defer.reject({
          message: 'Please provide a name for the Adapter to be saved as draft'
        });
        return defer.promise;
      }
      var adapterDrafts = {};
      this.adapterDrafts[this.metadata.name] = {
        config: {
          metadata: this.metadata,
          source: this.source,
          transforms: this.transforms,
          sink: this.sink,
          schedule: this.schedule
        }
      };

      return mySettings.set('adapterDrafts', this.adapterDrafts);
    }

    Model.prototype.save = function save() {
      var defer = $q.defer();
      if (this.source.placeHolder || this.sink.placeHolder) {
        defer.reject({
          message: 'Adapter needs atleast a source and a sink'
        });
        return defer.promise;
      } else {
        if (!this.validateRequiredProperties()) {
          defer.reject({
            message: 'All required fields need to be set for all plugins.'
          });
          return defer.promise;
        }
        return formatAndSave.bind(this)();
      }
    };

    Model.prototype.checkForValidRequiredField = function checkForValidRequiredField(plugin) {
      var i;
      var keys = Object.keys(plugin.properties);
      plugin.valid = true;
      for (i=0; i< keys.length; i++) {
        var property = plugin.properties[keys[i]];
        if (plugin._backendProperties[keys[i]].required && (!property || property === '')) {
          plugin.valid = false;
          break;
        }
      }
      return plugin.valid;
    }

    Model.prototype.validateRequiredProperties = function() {
      var isValidPlugin = this.checkForValidRequiredField(this.source);
      isValidPlugin = this.checkForValidRequiredField(this.sink) && isValidPlugin;
      this.transforms.forEach(function(transform) {
        if (!transform.placeHolder) {
          isValidPlugin = this.checkForValidRequiredField(transform) && isValidPlugin;
        }
      }.bind(this));

      return isValidPlugin;

    };

    function formatAndSave() {
      var defer = $q.defer();
      var source = angular.copy(this.source),
          sink = angular.copy(this.sink),
          trans = angular.copy(this.transforms),
          transforms = [];

      angular.forEach(source.properties, pruneProperties.bind(this, source));
      angular.forEach(sink.properties, pruneProperties.bind(this, sink));

      for (i=0; i<trans.length; i++) {
        angular.forEach(trans[i].properties, pruneProperties.bind(this, trans[i]));
        if (!trans[i].placeHolder) {
          delete trans[i]._backendProperties;
          delete trans[i].$$hashkey;
          transforms.push(trans[i]);
        }
      }

      var data = {
        template: this.metadata.type,
        description: this.metadata.description,
        config: {
          source: source,
          sink: sink,
          transforms: transforms
        }
      };
      if (this.metadata.type === 'ETLRealtime') {
        data.config.instances = 1;
      } else if (this.metadata.type === 'ETLBatch') {
        // default value should be * * * * *
        data.config.schedule = this.schedule.cron;
      }
      return AdapterApiFactory.save(
        {
          namespace: $state.params.namespace,
          adapter: this.metadata.name
        },
        data
      )
        .$promise
        .then(function(res) {
          delete this.adapterDrafts[this.metadata.name];
          return mySettings.set('adapterdrafts', this.adapterDrafts);
        }.bind(this));
    }

    function pruneProperties(plugin, value, key) {
      var match = plugin._backendProperties[key];
      if (match && match.required === false && (value === null || value === '')) {
        delete plugin.properties[key];
      }
    }

    Model.prototype.getDrafts = function() {
      var defer = $q.defer();
      return mySettings.get('adapterDrafts')
        .then(function(res) {
          this.adapterDrafts = res;
          defer.resolve(this.adapterDrafts);
          return defer.promise;
        }.bind(this));
    };

    return Model;

  });
