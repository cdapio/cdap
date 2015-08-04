angular.module(PKG.name + '.commons')
  .controller('navbarCtrl', function (myAlert, MYAUTH_EVENT, myNamespace, $scope, EventPipe) {

    $scope.namespaces = [];

    function updateNamespaceList() {
      myNamespace.getList()
        .then(function(list) {
          $scope.namespaces = list;
        });
    }

    // Listening for event from namespace create or namespace delete
    EventPipe.on('namespace.update', function() {
      updateNamespaceList();
    });

    $scope.$on (MYAUTH_EVENT.loginSuccess, updateNamespaceList);
    $scope.getDisplayName = myNamespace.getDisplayName.bind(myNamespace);

    $scope.$on (MYAUTH_EVENT.logoutSuccess, function () {
      $scope.namespaces = [];
    });

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
