angular.module(PKG.name + '.commons')
  .controller('myTabsCtrl', function($scope, $state) {
    $scope.tabs = $scope.tabs().map(function (t){
      return {
        title: t,
        state: t.toLowerCase(),
        partial: $scope.tabsPartialPath + t.toLowerCase() + '.html'
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
    // FIXME: For some weird reason it doesn't default to the first tab.
    $scope.tabs.activeTab = 0;
  });
