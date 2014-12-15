angular.module(PKG.name + '.feature.applications')
  .controller('ApplicationController', function ($scope, $state, MyDataSource) {
    $scope.tabs = [
      'Status',
      'Data',
      'Metadata',
      'Schedules',
      'History',
      'Resource',
      'Manage'
    ].map(function (t){
      return {
        title: t,
        state: t.toLowerCase(),
        partial: '/assets/features/applications/templates/tabs/' + t.toLowerCase() + '.html'
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

    var data = new MyDataSource($scope);
    var appId = $state.params.appId;
    data.fetch({
      _cdap: 'GET /apps/' + appId + '/status'
    }, function(res) {
      $scope.programs = res;
      $scope.programs.runningCount = getProgramCount($scope.programs, 'ALIVE');
      $scope.programs.failedCount = getProgramCount($scope.programs, 'FAILED');
      $scope.programs.deployedCount = getProgramCount($scope.programs, 'DEPLOYED');
    });

    function getProgramCount (programs, s) {
      return programs.filter(function(prog) {
        return prog.status === s;
      }).length;
    }
});
