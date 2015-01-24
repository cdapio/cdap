angular.module(PKG.name + '.feature.mapreduce')
  .controller('CdapMapreduceDetailController', function($scope, $state) {
    $scope.tabs = [
      'Runs',
      'Schedules',
      'Metadata',
      'History',
      'Resources'
    ].map(function (t){
      return {
        title: t,
        state: t.toLowerCase(),
        partial: '/assets/features/mapreduce/templates/tabs/' + t.toLowerCase() + '.html'
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
