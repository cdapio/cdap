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
      var path = '/namespaces/' + $state.params.namespace + '/streams/' + $scope.streamId + '/batch';

      for (var i = 0; i < files.length; i++) {
        myFileUploader.uploadCSV(files[i], path)
          .then(function () {
            $alert({
              type: 'success',
              title: 'Upload success',
              content: 'The csv has been uploaded successfully'
            });
          });
      }


    };
  });
