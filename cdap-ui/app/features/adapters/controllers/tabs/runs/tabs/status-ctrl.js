angular.module(PKG.name + '.feature.adapters')
  // TODO: We should use rAdapterDetail here since this data is already resolved at adapter.detail state
  .controller('AdapterRunDetailStatusController', function($scope, $state, myAdapterApi, $bootstrapModal, PluginConfigFactory, AdapterCreateModel) {

    $scope.transforms = [{
      name: '',
      properties: {},
      type: ''
    }];
    $scope.source = {
      name: '',
      properties: {},
      type: ''
    };
    $scope.sink = {
      name: '',
      properties: {},
      type: ''
    };

    var params = {
      namespace: $state.params.namespace,
      adapter: $state.params.adapterId,
      scope: $scope
    };

    var template;

    myAdapterApi.get(params)
      .$promise
      .then(function(res) {
        console.log('res', res);
        $scope.source = res.config.source;
        $scope.sink = res.config.sink;
        $scope.transforms = res.config.transforms || [];
        template = res.template;

        initializeProperties();

      });

    // Getting _backendProperties for Sink, Source, and Transforms
    function initializeProperties() {
      myAdapterApi
        .fetchSourceProperties({
          scope: $scope,
          adapterType: template,
          source: $scope.source.name
        })
        .$promise
        .then(function(res) {
          var pluginProperties = (res.length? res[0].properties: {});
          $scope.source._backendProperties = pluginProperties;
        });


      angular.forEach($scope.transforms, function(transform) {
        myAdapterApi
          .fetchTransformProperties({
            scope: $scope,
            adapterType: template,
            transform: transform.name
          })
          .$promise
          .then(function(res) {
            var pluginProperties = (res.length? res[0].properties : {});
            transform._backendProperties = pluginProperties;
          });
      });


      myAdapterApi
        .fetchSinkProperties({
          scope: $scope,
          adapterType: template,
          sink: $scope.sink.name
        })
        .$promise
        .then(function(res) {
          var pluginProperties = (res.length? res[0].properties : {});
          $scope.sink._backendProperties = pluginProperties;
        });
    }

    $scope.openProperties = function (plugin) {
      $bootstrapModal.open({
        animation: false,
        templateUrl: '/assets/features/adapters/templates/tabs/runs/tabs/properties/properties.html',
        controller: 'modalController',
        size: 'lg',
        resolve: {
          AdapterModel: function () {
            return plugin;
          },
          type: function () {
            return template;
          }
        }
      });
    };

  })
  .controller('modalController', function ($scope, $modalInstance, AdapterModel, type){
    $scope.plugin = AdapterModel;
    $scope.type = type;
    $scope.isDisabled = true;
  });
