angular.module(PKG.name + '.feature.adapters')
  .controller('_AdapterCreateController', function(MyPlumbService, myAdapterApi, $bootstrapModal, $scope, rConfig) {
    if (rConfig) {
      this.data =  rConfig;
    }
    this.metadata = MyPlumbService.metadata;

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

      $bootstrapModal.open({
        templateUrl: '/assets/features/adapters/templates/create/metadata.html',
        size: 'lg',
        keyboard: true,
        controller: ['$scope', function($scope) {
          $scope.metadata = this.metadata;
        }.bind(this)]
      });
    };

  });
