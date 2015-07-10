angular.module(PKG.name + '.feature.adapters')
  .controller('_AdapterCreateController', function(MyPlumbService, myAdapterApi, $bootstrapModal, $scope) {
    this.metadata = MyPlumbService.metadata;
    myAdapterApi.fetchTemplates({
      scope: $scope
    })
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
          $scope.metadata = this.metadata;
        }.bind(this)]
      });
    };
  });
