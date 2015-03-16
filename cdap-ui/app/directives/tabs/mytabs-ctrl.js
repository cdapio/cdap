angular.module(PKG.name + '.commons')
  .controller('myTabsCtrl', function($scope, $state) {
    $scope.tabs = $scope.tabsList().map(function (t){
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

    function checkTabParam (event, state) {
      var tab = $scope.tabs
        .map(function(t) {
          return t.state;
        })
        .indexOf($state.params.tab);

      if((tab < 0 || tab>=$scope.tabs.length)) {
        tab = 0;
      }
      $scope.tabs.activeTab = tab;
    }

    $scope.$on('$stateChangeSuccess', checkTabParam);
    checkTabParam();
  });
