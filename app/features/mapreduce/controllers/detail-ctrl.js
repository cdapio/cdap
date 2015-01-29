angular.module(PKG.name + '.feature.mapreduce')
  .controller('CdapMapreduceDetailController', function($scope, $state) {
    $scope.tabs = [
      'Runs',
      'Schedules',
      'Metadata',
      'History',
      'Resources'
    ];
    $scope.tabsPartialPath = '/assets/features/mapreduce/templates/tabs/';
    
  });
