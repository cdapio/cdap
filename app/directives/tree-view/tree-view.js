angular.module(PKG.name + '.commons')
  .directive('myTreeView', function(RecursionHelper) {
    return {
      restrict: 'E',
      scope: {
        treeList: '='
      },
      templateUrl: 'tree-view/tree-view.html',
      compile: function(element) {
        return RecursionHelper.compile(element);
      },
      controller: function($scope) {
        $scope.showSubMenu = false;
      }
    }
  })
