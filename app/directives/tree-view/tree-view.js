angular.module(PKG.name + '.commons')
  .directive('myTreeView', function(RecursionHelper) {
    return {
      restrict: 'E',
      scope: {
        model: '='
      },
      templateUrl: 'tree-view/tree-view.html',
      compile: function(element) {
        return RecursionHelper.compile(element);
      },
      controller: function($scope, $state) {
        $scope.isOpen = {};
        function setCurrent() {
          if (angular.isArray($scope.model)) {
            $scope.menus = $scope.model.map(function (menu) {
              var a = menu.state.split('(');
              return angular.extend({
                isCurrent: $state.includes(a[0]+'.**', '(' + a[1])
              }, menu);

            });
          }
        }

        // TO-DO: replace setCurrent arg with function that calls setCurrent and used label to set isOpen
        $scope.$on('$stateChangeSuccess', setCurrent) ;
        $scope.$watch('model', setCurrent);

      }
    };
  });
