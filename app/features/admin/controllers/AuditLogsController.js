angular.module(PKG.name + '.feature.admin')
  .controller('AuditLogsController', function ($scope) {
    $scope.logs = [{
      dateTime: '01/28/2015',
      version: '1.0',
      action: 'test',
      user: 'xyz',
      status: 'inactive'
    }];
  });

