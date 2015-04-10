angular.module(PKG.name + '.feature.etlapps')
  .controller('ETLAppsCreateController', function($scope, MyDataSource, $filter, $alert) {
    var filterFilter = $filter('filter');
    var dataSrc = new MyDataSource($scope);
    $scope.loadingEtlSourceProps = false;
    $scope.loadingEtlSinkProps = false;
    $scope.etlSources = [];
    $scope.etlSinks = [];
    $scope.etlTransforms = [];
    $scope.addProperty = function(type) {
      if ($scope[type]) {
        $scope[type].properties[type + '-' + Date.now()] = '';
      }
    };

    $scope.remoteProperty = function(type, propertyName) {
      if ($scope[type] && $scope[type].properties[propertyName]) {
        delete $scope[type].properties[propertyName];
      }
    };

    $scope.etlTypes = [
      {
        name: 'Etl Batch',
        type: 'batch'
      },
      {
        name: 'ETL Real Time',
        type: 'realtime'
      }
    ];
    $scope.metadata = {
        name: '',
        description: '',
        type: ''
    };

    $scope.$watch('metadata.type',fetchSources);

    function fetchSources(etlType) {
      if (!etlType) return;
      console.log("ETLType: ", etlType);
      dataSrc.request({
        _cdapPath: '/templates/etl.' + etlType + '/sources'
      })
        .then(function(res) {
          $scope.etlSources = res;
        });
      fetchSinks(etlType);
      fetchTransforms(etlType);
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

    $scope.source = {
      name: '',
      properties: {}
    };

    $scope.$watch('source.name', fetchSourceProperties);

    function fetchSourceProperties(etlSource) {
      if (!etlSource) return;
      dataSrc.request({
        _cdapPath: '/templates/etl.' + $scope.metadata.type + '/sources/' + etlSource
      })
        .then(function(res) {
          $scope.source = res;
          angular.forEach($scope.source.properties, function(property) {
            property.value = '';
          });
          $scope.loadingEtlSourceProps = false;
        });
      $scope.loadingEtlSourceProps = etlSource || false;
    }

    $scope.sink = {
      name: '',
      properties: {}
    };

    $scope.$watch('sink.name', fetchSinkProperties);

    function fetchSinkProperties(etlSink){
      if (!etlSink) return;
      console.log("Sink: ", etlSink);
      dataSrc.request({
        _cdapPath: '/templates/etl.' + $scope.metadata.type + '/sinks/' + etlSink
      })
        .then(function(res) {
          $scope.sink = res;
          angular.forEach($scope.sink.properties, function(property) {
            property.value = '';
          });
          $scope.loadingEtlSinkProps = false;
        });
      $scope.loadingEtlSinkProps = etlSink || false;
    }

    $scope.transforms = [];

    $scope.$watchCollection('transforms', function(newVal) {
      console.log("Transform Collection Watch", newVal);
    })

    $scope.handleSourceDrop = function(id, dropZone) {
      console.log("Source Dropped", id);
      if (dropZone.indexOf('source') === -1) {
        $alert({
          type: 'danger',
          content: 'Boink! You are not adding a source to a Source!'
        });
      } else {
        $alert({
          type: 'success',
          content: 'You have dropped a source!'
        });
      }
    };
    $scope.handleTransformDrop = function(id, dropZone) {
      console.log("Transform Dropped", id);
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
      }
    };
    $scope.handleSinkDrop = function(id, dropZone) {
      console.log("Sink Dropped", id);
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
      }
    };
  });
