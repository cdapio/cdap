angular.module(PKG.name + '.services')
  .service('myStreamService', function($bootstrapModal, $rootScope) {
    var modalInstance;

    this.show = function(streamId) {
			var scope = $rootScope.$new();
			scope.streamId = streamId;
      modalInstance = $bootstrapModal.open({
        controller: 'FlowStreamDetailController',
        templateUrl: '/assets/features/flows/templates/tabs/runs/streams/detail.html',
        scope: scope
      });
      return modalInstance;
    };

    this.hide = function() {
      modalInstance.hide();
    };

    this.dismiss = function() {
      modalInstance.dismiss();
    };

  })
  .controller('FlowStreamDetailController', function($scope, myStreamApi, $state, myFileUploader, $alert) {

    $scope.loading = false;

    $scope.doInject = function () {
      if(!$scope.userInput) {
        $scope.userInput = null;
        return;
      }

      var params = {
        namespace: $state.params.namespace,
        streamId: $scope.streamId,
        scope: $scope
      };

      var lines = $scope.userInput.replace(/\r\n/g, '\n').split('\n');

      angular.forEach(lines, function (line) {
        myStreamApi.sendEvent(params, line);
      });

      $scope.userInput = null;
    };

    $scope.dismiss = function() {
      $scope.$dismiss();
    };

    $scope.uploadFile = function (files) {
      $scope.loading = true;
      var path = '/namespaces/' + $state.params.namespace + '/streams/' + $scope.streamId + '/batch';

      for (var i = 0; i < files.length; i++) {
        // TODO: support other file types

        myFileUploader.upload({
          path: path,
          file: files[i]
        }, 'text/csv')
          .then(function () {
            $alert({
              type: 'success',
              title: 'Upload success',
              content: 'The file has been uploaded successfully'
            });

            $scope.loading = false;
          });
      }


    };
  });
