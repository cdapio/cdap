angular.module(PKG.name + '.commons')
  .controller('navbarCtrl', function($scope, $state) {

    $scope.navbarLinks = [
      {
        label: 'Development',
        parent: 'ns',
        sref: 'ns.overview',
        name: 'overview'
      },
      {
        sref: 'dashboard',
        label: 'Operations',
        name: 'ops'
      },
      {
        sref: 'admin.overview',
        label: 'Management',
        parent: 'admin',
        name: 'admin'
      }
    ];
    $scope.$watch('$state.params.namespaceId', function(newValue, oldValue) {
      if (newValue) {
        $scope.currentNamespace = newValue;
      }
    });
    $scope.onSelectNamespace = function(state) {
      $state.go('ns.overview', {namespaceId: state.name})
        .then(function() {
          $scope.currentNamespace = state.name;
        });
    };
    $scope.goToTab = function(tab) {
      if (tab.name === 'overview') {
          $state.go(tab.sref, {namespaceId: $scope.currentNamespace});
      } else {
        $state.go(tab.sref);
      }
    };

    $scope.doSearch = function () {
      $alert({
        title: 'Sorry!',
        content: 'Search is not yet implemented.',
        type: 'danger'
      });
    };
  });
