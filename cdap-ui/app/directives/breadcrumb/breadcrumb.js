angular.module(PKG.name+'.commons')
.directive('myBreadcrumb', function () {
  return {
    restrict: 'E',
    templateUrl: 'breadcrumb/breadcrumb.html',
    scope: {
      params: '='
    },
    controller: function($location, $scope) {
      var listener = $scope.$on('$stateChangeSuccess', function () {
        $location.search('sourceId', null);
        $location.search('sourceRunId', null);

        listener(); // removing listener, to make sure that this event only gets triggered once
      });
    }
  };

});
