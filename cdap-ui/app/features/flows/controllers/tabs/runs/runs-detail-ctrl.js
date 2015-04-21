angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunDetailController', function($scope, $state) {

    $scope.tabs = [{
      title: 'Status',
      template: '/assets/features/flows/templates/tabs/runs/tabs/status.html'
    },
    {
      title: 'Flowlets',
      template: '/assets/features/flows/templates/tabs/runs/tabs/flowlets.html'
    },
    {
      title: 'Logs',
      template: '/assets/features/flows/templates/tabs/runs/tabs/log.html'
    }];

    $scope.activeTab = $scope.tabs[0];

    $scope.$on('$destroy', function(event) {
      event.targetScope.runs.selected = null;
    });

    $scope.selectTab = function(tab, node) {
      if (tab.title === 'Flowlets') {
        $scope.activeFlowlet = node;
      }
      $scope.activeTab = tab;

    };
  });
