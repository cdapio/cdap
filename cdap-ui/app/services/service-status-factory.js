angular.module(PKG.name + '.services')
  .service('ServiceStatusFactory', function(MyDataSource, $alert, $timeout, EventPipe, $state, myAuth) {
    this.systemStatus = 'green';

    // Apart from invalid token there should be no scenario
    // when we should stop this poll.
    var dataSrc = new MyDataSource();
    dataSrc.poll({
      _cdapPath: '/system/services/status',
      interval: 10000
    },
    function success(res) {
      var serviceStatuses = Object.keys(res).map(function(value) {
        return res[value];
      });
      if (serviceStatuses.indexOf('NOTOK') > -1) {
        this.systemStatus = 'yellow';
      }
      if (serviceStatuses.indexOf('OK') === -1) {
        this.systemStatus = 'red';
      }
      if (serviceStatuses.indexOf('NOTOK') === -1) {
        this.systemStatus = 'green';
      }
    }.bind(this),
    function error(err) {
      // Check for invalid token if security is enabled.
      if (angular.isObject(err) && err.auth_uri) {
        $timeout(function() {
          EventPipe.emit('backendUp');
          myAuth.logout();
        });
      } else {
        EventPipe.emit('backendDown');
      }
    }
    );
  });
