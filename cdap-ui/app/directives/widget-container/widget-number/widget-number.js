angular.module(PKG.name + '.commons')
  .directive('myNumberWidget', function() {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        config: '='
      },
      template: '<input type="number" class="form-control" min="{{min}}" max="{{max}}" ng-model="model" />',
      controller: function($scope) {
        $scope.model = $scope.model || $scope.config.properties.default;
        $scope.min = $scope.config.min || '';
        $scope.max = $scope.config.max || '';
      }
    };
  });
