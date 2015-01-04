angular.module(PKG.name + '.commons')
  .controller('navbarCtrl', function ($alert, MYAUTH_EVENT, myNamespace, $scope, $state) {

    $scope.namespaces = [];

    $scope.$on (MYAUTH_EVENT.loginSuccess, function (event) {
      myNamespace.getList()
        .then(function(list) {
          $scope.namespaces = list;
        });
    });

    $scope.$on (MYAUTH_EVENT.logoutSuccess, function (event) {
      $scope.namespaces = [];
    });

    $scope.$on('$stateChangeSuccess', function(event, toState) {
      $scope.highlightTab = toState.data && toState.data.highlightTab;
    });

    $scope.doSearch = function () {
      $alert({
        title: 'Sorry!',
        content: 'Search is not yet implemented.',
        type: 'danger'
      });
    };
  });
