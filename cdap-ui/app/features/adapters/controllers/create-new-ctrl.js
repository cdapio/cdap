angular.module(PKG.name + '.feature.adapters')
  .controller('_AdapterCreateController', function(AdapterCreateModel, myAdapterApi, $bootstrapModal, $scope) {
    this.model = new AdapterCreateModel();
    this.model.metadata.name = '[Adapter Name]';
    myAdapterApi.fetchTemplates()
      .$promise
      .then(function(res) {
        this.adapterTypes = res;
      }.bind(this));
    this.showMetadataModal = function() {
      $bootstrapModal.open({
        templateUrl: '/assets/features/adapters/templates/create/metadata.html',
        size: 'lg',
        keyboard: true,
        controller: ['$scope', function($scope) {
          $scope.metadata = this.model.metadata;
        }.bind(this)]
      });
    };
  });
