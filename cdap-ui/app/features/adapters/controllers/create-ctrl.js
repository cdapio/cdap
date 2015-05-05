angular.module(PKG.name + '.feature.adapters')
  .controller('AdapterCreateController', function($scope, $q, $alert, $state, AdapterApiFactory, mySettings, $filter, $rootScope) {
    var apiFactory = new AdapterApiFactory($scope);

    // Loading flag to indicate source & sinks have
    // not been loaded yet (after/before choosing an etl template)
    // $scope.loadingEtlSourceProps = false;
    // $scope.loadingEtlSinkProps = false;
    $scope.onAdapterTypeSelected = false;

    // List of ETL Sources, Sinks & Transforms
    // for a particular etl template type fetched from backend.
    $scope.defaulSources = [];
    $scope.defaulSinks = [];
    $scope.defaulTransforms = [];
    $scope.selectedAdapterDraft = undefined;
    $scope.adaptersDraftList = [];

    $scope.onDraftChange = function(item, model) {
      var filterFilter = $filter('filter'),
          match = null,
          swapObj = {};
      if (!item) {
        return; //un-necessary.
      }
      if ($scope.adapterDrafts[item]) {
        $scope.metadata = $scope.adapterDrafts[item].config.metadata;
        $scope.source = $scope.adapterDrafts[item].config.source;
        $scope.sink = $scope.adapterDrafts[item].config.sink;
        $scope.transforms = $scope.adapterDrafts[item].config.transforms;
      } else {
        $scope.metadata.name = item;
        $scope.metadata.type = $scope.metadata.type;
        $scope.transforms = defaultTransforms;
        $scope.source = defaultSource;
        $scope.sink = defaultSink;
      }
    };

    apiFactory.fetchTemplates();
    $scope.adapterTypes = [];

    // Metadata Model
    $scope.metadata = {
        name: '',
        description: '',
        type: ''
    };

    $scope.schedule = {
      cron: ''
    };
    var defaultSource = {
      name: 'Add a source',
      properties: {},
      placeHolderSource: true
    };

    var defaultSink = {
      name: 'Add a sink',
      placeHolderSink: true,
      properties: {}
    };

    var defaultTransforms = [{
      name: 'Add a transform',
      placeHolderTransform: true,
      properties: {}
    }];

    // Source, Sink and Transform Models
    $scope.source = defaultSource;
    $scope.sink = defaultSink;
    $scope.transforms = defaultTransforms;
    $scope.activePanel = 0;

    $scope.$watch('metadata.type',function(adapterType) {

      if (!adapterType.length) {
        return;
      }
      $scope.onAdapterTypeSelected = true;
      apiFactory.fetchSources(adapterType);
      apiFactory.fetchSinks(adapterType);
      apiFactory.fetchTransforms(adapterType);
    }, true);

    $scope.handleSourceDrop = function(sourceName) {
      if ($scope.source.placeHolderSource) {
        delete $scope.source.placeHolderSource;
      }
      $scope.source.name = sourceName;
      apiFactory.fetchSourceProperties(sourceName);
    };
    $scope.handleTransformDrop = function(transformName) {
      var i,
          filterFilter = $filter('filter'),
          isPlaceHolderExist;
      isPlaceHolderExist = filterFilter($scope.transforms, {placeHolderTransform: true});
      if (isPlaceHolderExist.length) {
        for (i=0; i<$scope.transforms.length; i+=1) {
          if ($scope.transforms[i].placeHolderTransform) {
            $scope.transforms[i].name = transformName;
            delete $scope.transforms[i].placeHolderTransform;
            apiFactory.fetchTransformProperties(transformName, i);
            break;
          }
        }
        if (i === $scope.transforms.length) {
          $scope.transforms.push({
            name: transformName
          });
          apiFactory.fetchTransformProperties(transformName);
        }
      } else {
        $scope.transforms.push({
          name: transformName,
          properties: apiFactory.fetchTransformProperties(transformName)
        });
      }
    };
    $scope.handleSinkDrop = function(sinkName) {
      if ($scope.sink.placeHolderSink) {
        delete $scope.sink.placeHolderSink;
      }
      $scope.sink.name = sinkName;
      apiFactory.fetchSinkProperties(sinkName);
    };

    $scope.editSourceProperties = function() {
      if ($scope.source.placeHolderSource) {
        return;
      }
      var filterFilter = $filter('filter'),
          icon,
          match;
      match = filterFilter($scope.tabs, {type: 'source'});
      if (match.length) {
        $scope.tabs[$scope.tabs.indexOf(match[0])].active = true;
      } else {
        icon = filterFilter($scope.defaultSources, {name: $scope.source.name});
        $scope.tabs.push({
          title: $scope.source.name,
          icon: icon[0].icon,
          type: 'source',
          active: true,
          partial: '/assets/features/adapters/templates/create/tabs/sourcePropertyEdit.html'
        });
      }
    };
    $scope.editSinkProperties = function() {
      if ($scope.sink.placeHolderSink) {
        return;
      }

      var filterFilter = $filter('filter'),
          icon,
          match;
      match = filterFilter($scope.tabs, {type: 'sink'});
      if (match.length) {
        $scope.tabs[$scope.tabs.indexOf(match[0])].active = true;
      } else {
        icon = filterFilter($scope.defaultSinks, {name: $scope.sink.name});
        $scope.tabs.active = ($scope.tabs.push({
          title: $scope.sink.name,
          icon: icon[0].icon,
          type: 'sink',
          active: true,
          partial: '/assets/features/adapters/templates/create/tabs/sinkPropertyEdit.html'
        })) -1;
      }
    };
    $scope.editTransformProperty = function(transform) {
      if (transform.placeHolderTransform){
        return;
      }
      var filterFilter = $filter('filter'),
          match;
      match = filterFilter($scope.tabs, {
        transformid: transform.$$hashKey,
        type: 'transform'
      });
      if (match.length) {
        $scope.tabs[$scope.tabs.indexOf(match[0])].active = true;
      } else {
        icon = filterFilter($scope.defaultTransforms, {name: transform.name});
        $scope.tabs.active = ($scope.tabs.push({
          title: transform.name,
          icon: icon[0].icon,
          transformid: transform.$$hashKey,
          transform: transform,
          active: true,
          type: 'transform',
          partial: '/assets/features/adapters/templates/create/tabs/transformPropertyEdit.html'
        })) -1;
      }
    };

    $scope.deleteTransformProperty = function(transform) {
      var index = $scope.transforms.indexOf(transform);
      $scope.transforms.splice(index, 1);
      if (!$scope.transforms.length) {
        $scope.transforms.push({
          name: 'Add a Transforms',
          placeHolderTransform: true,
          properties: {}
        });
      }
    };

    $scope.doSave = function() {
      var source, trans,sink;
      var transforms = [],
          filterFilter = $filter('filter'),
          i;
      source = angular.copy($scope.source);
      sink = angular.copy($scope.sink);
      trans = angular.copy($scope.transforms);

      if ($scope.source.placeHolderSource || $scope.sink.placeHolderSource) {
        return;
      }

      angular.forEach(source.properties, function(value, key) {
        var match = source._backendProperties[key];
        if (match && match.required === false && value === null) {
          delete source.properties[key];
        }
      });
      angular.forEach(sink.properties, function(value, key) {
        var match = sink._backendProperties[key];
        if (match && match.required === false && value === null) {
          delete sink.properties[key];
        }
      });
      for (i=0; i<trans.length; i++) {
        angular.forEach(trans[i].properties, function(value, key) {
          var match = trans[i]._backendProperties[key];
          if (match && match.required === false && value === null) {
            delete trans[i].properties[key];
          }
        });

        if (!trans[i].placeHolderTransform) {
          delete trans[i]._backendProperties;
          delete trans[i].$$hashkey;
          transforms.push(trans[i]);
        }
      }

      var data = {
        template: $scope.metadata.type,
        description: $scope.metadata.description,
        config: {
          source: source,
          sink: sink,
          transforms: transforms
        }
      };
      if ($scope.metadata.type === 'ETLRealtime') {
        data.config.instances = 1;
      } else if ($scope.metadata.type === 'ETLBatch') {
        // default value should be * * * * *
        data.config.schedule = $scope.schedule.cron;
      }

      apiFactory.save(data);
    };

    $scope.dragdrop = {
      dragStart: function (drag) {
        console.log('dragStart', drag.source, drag.dest);
      },
      dragEnd: function (drag) {
        console.log('dragEnd', drag.source, drag.dest);
      }
    };

    $scope.getDrafts = function() {
      var defer = $q.defer();
      return mySettings.get('adapterDrafts')
        .then(function(res) {
          $scope.adapterDrafts = res || {};
          $scope.adaptersDraftList = Object.keys($scope.adapterDrafts);
          defer.resolve();
        });
    };
    if ($state.params.data) {
      $scope.getDrafts()
        .then(function() {
          $scope.selectedAdapterDraft = $state.params.data;
          $scope.onDraftChange($state.params.data);
        });
    }
    $scope.getDrafts();

    $scope.saveAsDraft = function() {
      if (!$scope.metadata.name.length) {
        $alert({
          type: info,
          content: 'Please provide a name for the Adapter to be saved as draft'
        });
        return;
      }
      $scope.adapterDrafts[$scope.metadata.name] = {
        config: {
          metadata: $scope.metadata,
          source: $scope.source,
          transforms: $scope.transforms,
          sink: $scope.sink
        }
      };

      mySettings.set('adapterDrafts', $scope.adapterDrafts)
      .then(function(res) {
        $scope.isSaved = true;
        $alert({
          type: 'success',
          content: 'The Adapter Template ' + $scope.metadata.name + ' has been saved as draft!'
        });
        $state.go('^.list');
      });
    };

    $scope.tabs = [
      {
        title: 'Default',
        icon: 'cogs',
        isCloseable: false,
        partial: '/assets/features/adapters/templates/create/tabs/default.html'
      }
    ];

    $scope.closeTab = function(index) {
      $scope.tabs.splice(index, 1);
    };

    $scope.$on('$stateChangeStart', function(event, toState, toParams, fromState, fromParams) {
      if (fromState.name === 'adapters.create' && !$scope.isSaved) {
        if(!confirm("Are you sure you want to leave this page?")) {
          event.preventDefault();
        }
      }
    });
  });
