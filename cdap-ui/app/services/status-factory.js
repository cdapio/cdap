angular.module(PKG.name + '.services')
  .service('StatusFactory', function($http, EventPipe, myAuth, $rootScope, MYAUTH_EVENT, MY_CONFIG, $alert, $timeout) {

    this.startPolling = function () {
      beginPolling.bind(this)();
    };
    function beginPolling() {

      _.debounce(function() {
        $http.get(
          (MY_CONFIG.sslEnabled? 'https://': 'http://')
          + window.location.host
          + '/backendstatus',
          {ignoreLoadingBar: true}
        )
             .success(success.bind(this))
             .error(error.bind(this));
      }.bind(this), 2000)();

    }

    function success() {
      EventPipe.emit('backendUp');
      this.startPolling();
    }

    function error(err) {
      if (!reAuthenticate(err)) {
        this.startPolling();
        EventPipe.emit('backendDown');
      }
    }



    function reAuthenticate(err) {
      if (angular.isObject(err) && err.auth_uri) {
        $timeout(function() {
          EventPipe.emit('backendUp');
          myAuth.logout();
        });
        return true;
      }
      return false;
    }

    // this.startPolling();
  });
