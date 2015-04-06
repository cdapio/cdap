angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceRunsDetailController', function($scope, MyDataSource, $state) {
    $scope.tabs = [{
      title: 'Status',
      template: '/assets/features/mapreduce/templates/tabs/runs/tabs/status.html'
    },
    {
      title: 'Logs',
      template: '/assets/features/mapreduce/templates/tabs/runs/tabs/log.html'
    }];
  });
