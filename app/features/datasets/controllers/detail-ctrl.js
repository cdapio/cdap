angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetDetailController', function CdapAppDetail($scope, $state, MyDataSource) {
    $scope.tabs = [
      'Status',
      'Explore',
      'Programs',
      'Inner',
      'Metadata',
      'Lineage',
      'Resources'
    ].map(function (t){
      return {
        title: t,
        state: t.toLowerCase(),
        partial: '/assets/features/datasets/templates/tabs/' + t.toLowerCase() + '.html'
      };
    });

    $scope.$watch('tabs.activeTab', function (newVal) {
      $state.go($state.includes('**.tab') ? $state.current : '.tab', {
        tab: $scope.tabs[newVal].state
      });
    });

    $scope.$on('$stateChangeSuccess', function (event, state) {
      var tab = $scope.tabs
        .map(function(t) {
          return t.state;
        })
        .indexOf($state.params.tab);

      if((tab < 0 || tab>=$scope.tabs.length)) {
        tab = 0;
      }
      $scope.tabs.activeTab = tab;
    });
});
