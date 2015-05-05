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
  .controller('FlowStreamDetailController', function($scope, MyDataSource) {

    var dataSrc = new MyDataSource($scope);


    $scope.doInject = function () {
      if(!$scope.userInput) {
        $scope.userInput = null;
        return;
      }

      dataSrc.request({
        _cdapNsPath: '/streams/' + $scope.streamId,
        method: 'POST',
        body: $scope.userInput,
        json: false
      });
      $scope.userInput = null;
    };


    $scope.dismiss = function() {
      $scope.$dismiss();
    };
  });
