angular.module(PKG.name + '.feature.flows')
  .controller('FlowStreamDetailController', function($state, $scope, MyDataSource) {

    var dataSrc = new MyDataSource($scope);


    $scope.doInject = function (event) {
      if(!$scope.userInput) {
        $scope.userInput = null;
        return;
      }
      
      dataSrc.request({
        _cdapNsPath: '/streams/' + $scope.streamId,
        method: 'POST',
        body: $scope.userInput
      });
      $scope.userInput = null;
    };


    $scope.dismiss = function() {
      $scope.$dismiss();
    };
  });
