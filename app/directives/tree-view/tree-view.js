angular.module(PKG.name + '.commons')
  .directive('treeView', function(RecursionHelper) {
    return {
      strict: 'E',
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
