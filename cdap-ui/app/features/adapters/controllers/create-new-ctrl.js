angular.module(PKG.name + '.feature.adapters')
  .controller('_AdapterCreateController', function(MyPlumbService, myAdapterApi, $bootstrapModal, $scope, rConfig, $stateParams, $alert, $modalStack) {
    this.metadata = MyPlumbService['metadata'];
    function resetMetadata() {
      this.metadata = MyPlumbService['metadata'];
    }

    MyPlumbService.registerResetCallBack(resetMetadata.bind(this));

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

      $bootstrapModal
        .open({
          templateUrl: '/assets/features/adapters/templates/create/metadata.html',
          size: 'lg',
          windowClass: 'adapter-modal',
          keyboard: true,
          controller: ['$scope', 'metadata', 'MyPlumbService', function($scope, metadata, MyPlumbService) {
            $scope.modelCopy = angular.copy(this.metadata);
            $scope.metadata = metadata;
            $scope.reset = function () {
              metadata['name'] = $scope.modelCopy.name;
              metadata['description'] = $scope.modelCopy.description;
            }.bind(this);


            $scope.$on('modal.closing', function (event, reason) {
              if ((reason === 'cancel' || reason === 'escape key press') && !$scope.confirm ) {
                var stringCopy = JSON.stringify($scope.modelCopy);
                var stringPlugin = JSON.stringify($scope.metadata);

                if (stringCopy !== stringPlugin) {
                  event.preventDefault();

                  var confirmInstance = $bootstrapModal.open({
                    keyboard: false,
                    templateUrl: '/assets/features/adapters/templates/partial/confirm.html',
                    windowClass: 'modal-confirm',
                    controller: ['$scope', function ($scope) {
                      $scope.continue = function () {
                        $scope.$close('close');
                      };

                      $scope.cancel = function () {
                        $scope.$close('keep open');
                      };
                    }]
                  });

                  confirmInstance.result.then(function (closing) {
                    if (closing === 'close') {
                      $scope.confirm = true;
                      $scope.reset();
                      $scope.$close('cancel');
                    }
                  });
                }
              }
            });

          }.bind(this)],
          resolve: {
            metadata: function() {
              return this['metadata'];
            }.bind(this)
          }
        })
        .result
        .finally(function() {
          MyPlumbService.metadata.name = this.metadata.name;
          MyPlumbService.metadata.description = this.metadata.description;
        }.bind(this));
    };

    $scope.$on('$destroy', function() {
      $modalStack.dismissAll();
    });
  });
