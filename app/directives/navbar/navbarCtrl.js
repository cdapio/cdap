angular.module(PKG.name + '.commons')
  .controller('navbarCtrl', function ($alert, MYAUTH_EVENT, myNamespace, $scope, $state) {

    $scope.namespaces = [];

    function updateNamespaceList() {
      myNamespace.getList()
        .then(function(list) {
          $scope.namespaces = list;
        });
    }

    $scope.$on (MYAUTH_EVENT.loginSuccess, updateNamespaceList);
    $scope.getDisplayName = myNamespace.getDisplayName.bind(myNamespace);

    $scope.$on (MYAUTH_EVENT.logoutSuccess, function (event) {
      $scope.namespaces = [];
    });

    $scope.$on('$stateChangeSuccess', function(event, toState, toParams, fromState) {
      $scope.highlightTab = toState.data && toState.data.highlightTab;
      // This is required when user creates a new namespace in admin section.
      // As of now $dropdown doesn't have broadcast event for click.
      if (fromState.name === "admin.namespace.create") {
        updateNamespaceList();
      }
    });

    $scope.doSearch = function () {
      $alert({
        title: 'Sorry!',
        content: 'Search is not yet implemented.',
        type: 'danger'
      });
    };
  });
