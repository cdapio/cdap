angular.module(PKG.name + '.commons')
  .directive('myJsonTextbox', function() {
    return {
      restrict: 'EA',
      scope: {
        model: '=ngModel',
        placeholder: '='
      },
      template: '<textarea class="form-control" cask-json-edit="internalModel" placeholder="placeholder"></textarea>',
      controller: function($scope) {

        try {
          $scope.internalModel = JSON.parse($scope.model);
        } catch(e) {
          $scope.internalModel = "";
        }

        $scope.$watch('internalModel', function(newVal, oldVal) {
          if (newVal !== oldVal) {
            $scope.model = angular.toJson($scope.internalModel);
          }
        });
      }
    };
  });
