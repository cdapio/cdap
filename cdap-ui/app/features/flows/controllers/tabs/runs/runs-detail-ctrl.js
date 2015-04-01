angular.module(PKG.name + '.feature.flows')
  .controller('RunDetail', function($scope) {
    $scope.tabs = [{
      title: 'Status',
      template: '/assets/features/flows/templates/tabs/runs/tabs/status.html'
    },
    {
      title: 'Logs',
      template: '/assets/features/flows/templates/tabs/runs/tabs/log.html'
    }];
  });
