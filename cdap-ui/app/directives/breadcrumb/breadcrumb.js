angular.module(PKG.name+'.commons')
.directive('myBreadcrumb', function () {
  return {
    restrict: 'E',
    templateUrl: 'breadcrumb/breadcrumb.html',
    scope: {
      params: '='
    },
    controller: function($rootScope, $location, $scope) {
      var listener = $scope.$on('$stateChangeSuccess', function () {
        $location.search('sourceId', null);
        $location.search('sourceRunId', null);

        listener();
      });
    }
  };

});
