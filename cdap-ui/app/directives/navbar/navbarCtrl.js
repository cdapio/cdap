angular.module(PKG.name + '.commons')
  .controller('navbarCtrl', function (myAlert, $scope) {

    $scope.$on('$stateChangeSuccess', function(event, toState) {
      $scope.highlightTab = toState.data && toState.data.highlightTab;
    });

    $scope.doSearch = function () {
      myAlert({
        title: 'Sorry',
        content: 'Search is not yet implemented.',
        type: 'danger'
      });
    };
  });
