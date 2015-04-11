angular.module(PKG.name + '.feature.etlapps')
  .controller('ETLAppsCreateController', function($scope, MyDataSource, $filter, $alert, $bootstrapModal, $state) {
    var filterFilter = $filter('filter');
    var dataSrc = new MyDataSource($scope);

    // Loading flag to indicate source & sinks have
    // not been loaded yet (after/before choosing an etl template)
    $scope.loadingEtlSourceProps = false;
    $scope.loadingEtlSinkProps = false;

    // List of ETL Sources, Sinks & Transforms
    // for a particular etl template type fetched from backend.
    $scope.etlSources = [];
    $scope.etlSinks = [];
    $scope.etlTransforms = [];

    // Default ETL Templates
    $scope.etlTypes = [
      {
        name: 'Etl Batch',
        type: 'etlbatch'
      },
      {
        name: 'ETL Real Time',
        type: 'realtime'
      }
    ];

    // Metadata Model
    $scope.metadata = {
        name: '',
        description: '',
        type: ''
    };

    // Source, Sink and Transform Models
    $scope.source = {
      name: '',
      properties: {}
    };

    $scope.sink = {
      name: '',
      properties: {}
    };

    $scope.transforms = [];
    $scope.activePanel = 0;

    $scope.$watch('metadata.type',function(etlType) {
      fetchSources(etlType);
      fetchSinks(etlType);
      fetchTransforms(etlType);
    });

    function fetchSources(etlType) {
      if (!etlType) return;
      dataSrc.request({
        _cdapPath: '/templates/etl.' + etlType + '/sources'
      })
        .then(function(res) {
          $scope.etlSources = res;
        });
    }

    function fetchSinks(etlType) {
      dataSrc.request({
        _cdapPath: '/templates/etl.'+ etlType + '/sinks'
      })
        .then(function(res) {
          $scope.etlSinks = res;
        });
    }

    function fetchTransforms(etlType) {
      dataSrc.request({
        _cdapPath: '/templates/etl.' + etlType + '/transforms'
      })
        .then(function(res) {
          $scope.etlTransforms = res;
        });
    }

    function fetchSourceProperties(etlSource) {
      if (!etlSource) return;
      dataSrc.request({
        _cdapPath: '/templates/etl.' + $scope.metadata.type + '/sources/' + etlSource
      })
        .then(function(res) {
          $scope.source.name = res.name;
          var obj = {};
          angular.forEach(res.properties, function(property) {
            obj[property.name] = '';
          });
          $scope.source.properties = obj;
          $scope.loadingEtlSourceProps = false;
        });
      $scope.loadingEtlSourceProps = etlSource || false;
    }

    function fetchSinkProperties(etlSink){
      if (!etlSink) return;
      dataSrc.request({
        _cdapPath: '/templates/etl.' + $scope.metadata.type + '/sinks/' + etlSink
      })
        .then(function(res) {
          $scope.sink.name = res.name;
          var obj = {};
          angular.forEach(res.properties, function(property) {
            obj[property.name] = '';
          });
          $scope.sink.properties = obj;
          $scope.loadingEtlSinkProps = false;
        });
      $scope.loadingEtlSinkProps = etlSink || false;
    }

    function fetchTransformProperties(etlTransform) {
      if(!etlTransform) return;
      dataSrc.request({
        _cdapPath: '/templates/etl.' + $scope.metadata.type + '/transforms/' + etlTransform
      })
        .then(function(res) {
          var obj = {};
          var match = filterFilter($scope.transforms, {name: res.name});
          if(match.length) {
            angular.forEach(res.properties, function(property) {
              obj[property.name] = '';
            });
            match[0].properties = obj;
          }
          $scope.transforms[$scope.transforms.indexOf(match[0])].properties = obj;
        });
    }

    $scope.handleSourceDrop = function(id, dropZone, sourceName) {
      if (dropZone.indexOf('source') === -1) {
        $alert({
          type: 'danger',
          content: 'Boink! You are not adding a source to a Source!'
        });
      } else {
        $alert({
          type: 'success',
          content: 'You have added a Source!!'
        });
        $scope.source.name = sourceName;
        fetchSourceProperties(sourceName);
      }
    };
    $scope.handleTransformDrop = function(id, dropZone, transformName) {
      if (dropZone.indexOf('transform') === -1) {
        $alert({
          type: 'danger',
          content: 'Boink! You are not adding a source to a Transform!'
        });
      } else {
        $alert({
          type: 'success',
          content: 'You have dropped a transform!'
        });
        $scope.transforms.push({
          name: transformName,
          properties: {}
        });
        fetchTransformProperties(transformName);
      }
    };
    $scope.handleSinkDrop = function(id, dropZone, sinkName) {
      if (dropZone.indexOf('sink') === -1) {
        $alert({
          type: 'danger',
          content: 'Boink! You are not adding a source to a Sink!'
        });
      } else {
        $alert({
          type: 'success',
          content: 'You have dropped a sink!'
        });
        $scope.sink.name = sinkName;
        fetchSinkProperties(sinkName);
      }
    };

    $scope.editSourceProperties = function() {
      if ($scope.source.name === '') {
        return;
      }
      $bootstrapModal.open({
        templateUrl: '/assets/features/etlapps/templates/create/sourceProperties.html',
        scope: $scope,
        backdrop: true,
        keyboard: true
      });
    };
    $scope.editSinkProperties = function() {
      if ($scope.sink.name === '') {
        return;
      }
      $bootstrapModal.open({
        templateUrl: '/assets/features/etlapps/templates/create/sinkProperties.html',
        scope: $scope,
        backdrop: true,
        keyboard: true

      });
    };
    $scope.editTransformProperties = function() {
      if ($scope.transforms.length === 0) {
        return;
      }
      $bootstrapModal.open({
        templateUrl: '/assets/features/etlapps/templates/create/transformProperties.html',
        scope: $scope,
        size: 'lg',
        backdrop: true,
        keyboard: true

      });
    };
    $scope.doSave = function() {
      var data = {
        template: $scope.metadata.type,
        config: {
          source: $scope.source,
          sink: $scope.sink,
          transforms: $scope.transforms
        }
      };

      dataSrc.request({
        method: 'PUT',
        _cdapPath: '/namespaces/'
                    + $state.params.namespace +
                    '/adapters/' +
                    $scope.metadata.name,
        body: data
      })
        .then(function(res) {
          $state.go('etlapps.list', $state.params, {reload: true});
          $alert({
            type: 'success',
            content: 'ETL Template: ' + $scope.metata.name + ' created successfully!'
          });
        });
    }
  });
