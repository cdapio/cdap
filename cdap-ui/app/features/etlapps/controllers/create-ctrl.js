angular.module(PKG.name + '.feature.etlapps')
  .controller('ETLAppsCreateController', function($scope, MyDataSource) {
    var dataSrc = new MyDataSource($scope);
    $scope.loadingEtlSourceProps = false;
    $scope.loadingEtlSinkProps = false;

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

    $scope.$watch('metadata.type',fetchSources)

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
      console.log("ETLSource: ", etlSource);
      dataSrc.request({
        _cdapPath: '/templates/etl.' + $scope.metadata.type + '/sources/' + etlSource
      })
        .then(function(res) {
          console.log("Source Name:", etlSource, "Properties:", res);
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
          console.log("Sink Name:", etlSink, "Properties:", res);
        });
      $scope.loadingEtlSinkProps = etlSink || false;
    }

    $scope.transforms = [];

    $scope.$watchCollection('transforms', function(newVal) {
      console.log("Transform Collection Watch", newVal);
    })

  });
