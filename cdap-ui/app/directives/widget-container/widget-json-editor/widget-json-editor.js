angular.module(PKG.name + '.commons')
  .directive('myJsonTextbox', function() {
    return {
      restrict: 'EA',
      scope: {
        model: '=ngModel',
        placeholder: '='
      },
      template: '<textarea class="form-control" data-ng-trim="false" cask-json-edit="internalModel" placeholder="placeholder"></textarea>',
      controller: function($scope, EventPipe) {
        var modelCopy = angular.copy($scope.model);
        function initialize () {
          try {
            $scope.internalModel = JSON.parse($scope.model);
          } catch(e) {
            $scope.internalModel = '';
          }
        }

        initialize();

        $scope.$watch('model', initialize);

        $scope.$watch('internalModel', function(newVal, oldVal) {
          if (newVal !== oldVal) {
            $scope.model = angular.toJson($scope.internalModel);
            modelCopy = angular.copy($scope.model);
          }
        });
        EventPipe.on('plugin.reset', function () {
          $scope.model = angular.copy(modelCopy);
        });
      }
    };
  });
