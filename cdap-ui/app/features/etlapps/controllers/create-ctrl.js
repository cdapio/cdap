angular.module(PKG.name + '.feature.etlapps')
  .controller('ETLAppsCreateController', function($scope, MyDataSource, $alert, $bootstrapModal, $state, ETLAppsApiFactory) {
    var apiFactory = new ETLAppsApiFactory($scope);
    // Loading flag to indicate source & sinks have
    // not been loaded yet (after/before choosing an etl template)
    $scope.loadingEtlSourceProps = false;
    $scope.loadingEtlSinkProps = false;
    $scope.onETLTypeSelected = false;

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
      if (!etlType) return;
      $scope.onETLTypeSelected = true;
      apiFactory.fetchSources(etlType);
      apiFactory.fetchSinks(etlType);
      apiFactory.fetchTransforms(etlType);
    });


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
        apiFactory.fetchSourceProperties(sourceName);
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
        apiFactory.fetchTransformProperties(transformName);
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
        apiFactory.fetchSinkProperties(sinkName);
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
      apiFactory.save(data);
    }
  });
