angular.module(PKG.name + '.feature.flows')
  .controller('WorkflowsRunsDetailController', function($scope) {
    $scope.tabs = [{
      title: 'Status',
      template: '/assets/features/workflows/templates/tabs/runs/tabs/status.html'
    }];
  });
