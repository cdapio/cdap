angular.module(PKG.name + '.services')
  .service('myNamespace', function myNamespace($q, MyDataSource, EventPipe, $http, $rootScope, myAuth, MYAUTH_EVENT, myHelpers, $state) {

    this.namespaceList = [];

    var data = new MyDataSource(),
        prom,
        queryInProgress = null;


    this.getList = function (force) {
      if (!force && this.namespaceList.length) {
          return $q.when(this.namespaceList);
      }

      if (!queryInProgress) {
        prom = $q.defer();
        queryInProgress = true;
        data.request(
          {
            _cdapPath: '/namespaces',
            method: 'GET'
          })
            .then(
              (function(res) {

                if(!res.length) {
                  res = [{
                    id: 'default',
                    name: 'default'
                  }];
                }

                this.namespaceList = res;
                EventPipe.emit('namespace.update');
                prom.resolve(res);
                queryInProgress = null;
              }).bind(this),
              function (err) {
                prom.reject(err);
                queryInProgress = null;
                /*
                  If security is enabled, we authenticate and validate token
                   in 'home' state. However when we are in a nested state
                   under namespace say, /ns/default/apps/PurchaseHistory/...
                   the authentication and token validation is not done and we fetch the
                   list of namespaces in 'ns' state in resolve with invalid token.

                   This leads to a 'invalid_token' error but we don't properly re-direct
                   to login. Hence this check.
                */
                if (myHelpers.objectQuery(err, 'data', 'auth_uri')) {
                  $rootScope.$broadcast(MYAUTH_EVENT.sessionTimeout);
                  $state.go('login');
                } else {
                  EventPipe.emit('backendDown', 'Problem accessing namespace.', 'Please refresh the page.');
                }
              }
        );

      }

      return prom.promise;
    };

    this.getDisplayName = function(name) {
      var ns = this.namespaceList.filter(function(namespace) {
        return namespace.name === name;
      });
      return ns[0].name || name;
    };

  });
