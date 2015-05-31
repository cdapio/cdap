angular.module(PKG.name + '.feature.adapters')
  .controller('DefaultCanvasController', function($scope, AdapterApiFactory, $q, $filter) {
    var createCtrl = $scope.AdapterCreateController;
    function formatProperties(properties) {
      var defer = $q.defer();
      var obj = {};
      angular.forEach(properties, function(property) {
        obj[property.name] = '';
      });
      defer.resolve(obj);
      return defer.promise;
    };

    this.addSource = function (sourceName) {
      var source = {
        name: sourceName
      };
      AdapterApiFactory
        .fetchSourceProperties(
          {
            scope: $scope,
            adapterType: createCtrl.model.metadata.type,
            source: sourceName
          }
        )
        .$promise
        .then(function(res) {
          source._backendProperties = res[0].properties;
          return formatProperties(res[0].properties);
        })
        .then(function(properties) {
          source.properties = properties;
          createCtrl.model.setSource(source);
        });
    };

    this.addTransform = function(transformName) {
      var transform = {
        name: transformName
      };
      AdapterApiFactory
        .fetchTransformProperties(
          {
            scope: $scope,
            adapterType: createCtrl.model.metadata.type,
            transform: transformName
          }
        )
        .$promise
        .then(function(res) {
          transform._backendProperties = res[0].properties;
          return formatProperties(res[0].properties);
        })
        .then(function(properties) {
          transform.properties = properties;
          createCtrl.model.setTransform(transform);
        });
    };

    this.addSink = function(sinkName) {
      var sink = {
        name: sinkName
      };
      AdapterApiFactory
        .fetchSinkProperties(
          {scope: $scope, adapterType: createCtrl.model.metadata.type, sink: sinkName})
        .$promise
        .then(function(res) {
          sink._backendProperties = res[0].properties;
          return formatProperties(res[0].properties);
        })
        .then(function(properties) {
          sink.properties = properties;
          createCtrl.model.setSink(sink);
        });
    };

    this.editSourceProperties = function() {
      if (createCtrl.model.source.placeHolderSource) {
        return;
      }
      var filterFilter = $filter('filter'),
          icon,
          match;
      match = filterFilter(createCtrl.tabs, {type: 'source'});
      if (match.length) {
        createCtrl.tabs[
          createCtrl.tabs.indexOf(match[0])
        ].active = true;
      } else {
        icon = filterFilter(
          createCtrl.defaultSources,
          {name: createCtrl.model.source.name}
        );
        createCtrl.tabs.push({
          title: createCtrl.model.source.name,
          icon: icon[0].icon,
          type: 'source',
          active: true,
          partial: '/assets/features/adapters/templates/create/tabs/edit-source-properties.html'
        });
      }
    };

    this.editSinkProperties = function() {
      if (createCtrl.model.sink.placeHolderSink) {
        return;
      }

      var filterFilter = $filter('filter'),
          icon,
          match;
      match = filterFilter(createCtrl.tabs, {type: 'sink'});
      if (match.length) {
        createCtrl.tabs[
          createCtrl.tabs.indexOf(match[0])
        ].active = true;
      } else {
        icon = filterFilter(
          createCtrl.defaultSinks,
          {name: createCtrl.model.sink.name}
        );
        createCtrl.tabs.active = (createCtrl.tabs.push({
          title: createCtrl.model.sink.name,
          icon: icon[0].icon,
          type: 'sink',
          active: true,
          partial: '/assets/features/adapters/templates/create/tabs/edit-sink-properties.html'
        })) -1;
      }
    };

    this.editTransformProperty = function(transform) {
      if (transform.placeHolderTransform){
        return;
      }
      var filterFilter = $filter('filter'),
          match;
      match = filterFilter(createCtrl.tabs, {
        transformid: transform.$$hashKey,
        type: 'transform'
      });
      if (match.length) {
        createCtrl.tabs[createCtrl.tabs.indexOf(match[0])].active = true;
      } else {
        icon = filterFilter(
          createCtrl.defaultTransforms,
          {name: transform.name}
        );
        createCtrl.tabs.active = (createCtrl.tabs.push({
          title: transform.name,
          icon: icon[0].icon,
          transformid: transform.$$hashKey,
          transform: transform,
          active: true,
          type: 'transform',
          partial: '/assets/features/adapters/templates/create/tabs/edit-transform-properties.html'
        })) -1;
      }
    };

    this.deleteTransform = function(transform) {
      var index = createCtrl.model.transforms.indexOf(transform);
      createCtrl.model.transforms.splice(index, 1);
      if (!createCtrl.model.transforms.length) {
        createCtrl.model.transforms.push({
          name: 'Add a Transform',
          placeHolder: true,
          properties: {}
        });
      }
    };

  });
