angular.module(PKG.name + '.feature.apps')
  .controller('CdapAppDetailController', function CdapAppDetail($scope, $state, MyDataSource) {
    $scope.tabs = [
      'Status',
      'Data',
      'Metadata',
      'Schedules',
      'History',
      'Resource',
      'Manage'
    ];

    $scope.tabsPartialPath = '/assets/features/apps/templates/tabs/';
});
