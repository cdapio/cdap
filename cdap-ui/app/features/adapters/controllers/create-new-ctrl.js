angular.module(PKG.name + '.feature.adapters')
  .controller('_AdapterCreateController', function(MyPlumbService, myAdapterApi, $bootstrapModal, $scope, rConfig, $stateParams, $alert) {
    var modalInstance;
    this.metadata = MyPlumbService.metadata;
    if (rConfig) {
      this.data =  rConfig;
    }
    if ($stateParams.name) {
      this.metadata.name = $stateParams.name;
    }
    if ($stateParams.type) {
      if (['ETLBatch', 'ETLRealtime'].indexOf($stateParams.type) !== -1) {
        this.metadata.template.type = $stateParams.type;
      } else {
        $alert({
          type: 'danger',
          content: 'Invalid template type. Has to be either ETLBatch or ETLRealtime'
        });
      }
    }

    myAdapterApi.fetchTemplates({
      scope: $scope
    })
      .$promise
      .then(function(res) {
        this.adapterTypes = res;
      }.bind(this));

    this.showMetadataModal = function() {
      if (this.metadata.error) {
        delete this.metadata.error;
      }

      modalInstance = $bootstrapModal.open({
        templateUrl: '/assets/features/adapters/templates/create/metadata.html',
        size: 'lg',
        keyboard: true,
        controller: ['$scope', function($scope) {
          $scope.metadata = this.metadata;
        }.bind(this)]
      });
    };

    $scope.$on('$destroy', function() {
      if (modalInstance) {
        modalInstance.close();
      }
    });
  });
