angular.module(PKG.name + '.feature.adapters')
  .controller('_AdapterCreateController', function(MyPlumbService, myAdapterApi, $bootstrapModal, $scope, rConfig, $stateParams, $alert, $modalStack, ModalConfirm, EventPipe, $window) {
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
      MyPlumbService.isConfigTouched = true;
      $bootstrapModal
        .open({
          templateUrl: '/assets/features/adapters/templates/create/metadata.html',
          size: 'lg',
          windowClass: 'adapter-modal',
          keyboard: true,
          controller: ['$scope', 'metadata', function($scope, metadata) {
            $scope.modelCopy = angular.copy(this.metadata);
            $scope.metadata = metadata;
            $scope.reset = function () {
              metadata['name'] = $scope.modelCopy.name;
              metadata['description'] = $scope.modelCopy.description;
            }.bind(this);

            function closeFn() {
              $scope.reset();
              $scope.$close('cancel');
            }

            ModalConfirm.confirmModalAdapter(
              $scope,
              $scope.metadata,
              $scope.modelCopy,
              closeFn
            );


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


    var confirmOnPageExit = function (e) {

      if (!MyPlumbService.isConfigTouched) { return; }
      // If we haven't been passed the event get the window.event
      e = e || $window.event;
      var message = 'You have unsaved changes.';
      // For IE6-8 and Firefox prior to version 4
      if (e) {
        e.returnValue = message;
      }
      // For Chrome, Safari, IE8+ and Opera 12+
      return message;
    };
    $window.onbeforeunload = confirmOnPageExit;

    $scope.$on('$stateChangeStart', function (event) {
      if (MyPlumbService.isConfigTouched) {
        var response = confirm('You have unsaved changes. Are you sure you want to exit this page?');
        if (!response) {
          event.preventDefault();
        }
      }
    });

    $scope.$on('$destroy', function() {
      $modalStack.dismissAll();
      $window.onbeforeunload = null;
      EventPipe.cancelEvent('plugin.reset');
      EventPipe.cancelEvent('schema.clear');
    });
  });
