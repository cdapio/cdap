angular.module(PKG.name + '.commons')
  .controller('breadcrumbsCtrl', function (MYAUTH_EVENT, myNamespace, $scope, EventPipe, $dropdown, $state) {
    // Namespace dropdown
    $scope.namespaces = [];
    $scope.hideNsDropdown = false;
    function updateNamespaceList() {
      myNamespace.getList()
        .then(function(list) {
          $scope.namespaces = list;
        });
    }
    updateNamespaceList();

    $scope.$watch('hideNsDropdown', function() {
      if (angular.isDefined($scope.hideNsDropdown)) {
        var toggles = $scope.element[0].querySelectorAll('a.ns-dropdown-toggle');
        var element;
         element = angular.element(toggles[0]);
        // namespace dropdown
        $dropdown(element, {
          template: 'breadcrumbs/namespace.html',
          animation: 'none',
          scope: $scope
        });
      }
    });

    // Listening for event from namespace create or namespace delete
    EventPipe.on('namespace.update', function() {
      updateNamespaceList();
    });

    $scope.$on (MYAUTH_EVENT.loginSuccess, updateNamespaceList);
    $scope.namespace = $state.params.namespace;
    $scope.$on (MYAUTH_EVENT.logoutSuccess, function () {
      $scope.namespaces = [];
    });

  });
