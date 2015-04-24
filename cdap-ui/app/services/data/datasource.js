angular.module(PKG.name+'.services')

  /**
    Example Usage:

    MyDataSource // usage in a controler:

    var dataSrc = new MyDataSource($scope);

    // polling a namespaced resource example:
    dataSrc.poll({
        method: 'GET',
        _cdapNsPath: '/foo/bar',
        interval: 5000 // in milliseconds.
      },
      function(result) {
        $scope.foo = result;
      }
    ); // will poll <host>:<port>/v3/namespaces/<currentNamespace>/foo/bar

    // posting to a systemwide resource:
    dataSrc.request({
        method: 'POST',
        _cdapPath: '/system/config',
        body: {
          foo: 'bar'
        }
      },
      function(result) {
        $scope.foo = result;
      }
    ); // will post to <host>:<port>/v3/system/config

   */

  .factory('MyDataSource', function ($state, $log, $rootScope, caskWindowManager, mySocket,
    MYSOCKET_EVENT, $q, $filter) {

    var instances = {}; // keyed by scopeid

    /**
     * Generates unique id's for each request that is being sent on
     * the websocket connection.
     */
    var generateUUID = function() {
      return uuid.v4();
    }

    /**
     * Start polling of the resource - sends the action 'poll-start' to
     * the node backend.
     */
    function _pollStart (resource) {
      mySocket.send({
        action: 'poll-start',
        resource: resource
      });
    }

    /**
     * Stops polling of the resource - sends the actions 'poll-stop' to
     * the node backend.
     */
    function _pollStop (resource) {
      mySocket.send({
        action: 'poll-stop',
        resource: resource
      });
    }

    $rootScope.$on(MYSOCKET_EVENT.reconnected, function () {
      $log.log('[DataSource] reconnected, reloading...');

      // https://github.com/angular-ui/ui-router/issues/582
      $state.transitionTo($state.current, $state.$current.params,
        { reload: true, inherit: true, notify: true }
      );
    });

    function DataSource (scope) {
      scope = scope || $rootScope.$new();

      var id = scope.$id,
          self = this;

      if(instances[id]) {
        throw new Error('multiple DataSource for scope', id);
      }
      instances[id] = self;

      this.bindings = [];

      scope.$on(MYSOCKET_EVENT.message, function (event, data) {
        if(data.statusCode>299 || data.warning) {
          angular.forEach(self.bindings, function (b) {
            if(b.resource.id === data.resource.id) {
              if(b.errorCallback) {
                scope.$apply(b.errorCallback.bind(null, data));
              }
            }
          });
          return; // errors are handled at $rootScope level
        }
        angular.forEach(self.bindings, function (b) {
          if(b.resource.id === data.resource.id) {
            scope.$apply(b.callback.bind(null, data.response));
          }
        });
      });

      scope.$on('$destroy', function () {
        delete instances[id];
      });

      scope.$on(caskWindowManager.event.blur, function () {
        angular.forEach(self.bindings, function (b) {
          if(b.poll) {
            _pollStop(b.resource);
          }
        });
      });

      scope.$on(caskWindowManager.event.focus, function () {
        angular.forEach(self.bindings, function (b) {
          if(b.poll) {
            _pollStart(b.resource);
          }
        });
      });

      this.scope = scope;
    }

    /**
     * Start polling of a resource when in scope.
     */
    DataSource.prototype.poll = function (resource, cb, errorCb) {
      var id = generateUUID()
      resource.id = id;
      this.bindings.push({
        poll: true,
        resource: resource,
        id: id,
        callback: cb,
        errorCallback: errorCb
      });

      this.scope.$on('$destroy', function () {
        _pollStop(resource);
      });

      _pollStart(resource);
      return id;
    };

    /**
     * Stop polling of a resource when requested or when out of scope.
     */
    DataSource.prototype.stopPoll = function(id) {
      var filterFilter = $filter('filter');
      var match = filterFilter(this.bindings, {id: id});

      if (match.length) {
        _pollStop(match[0].resource);
        this.bindings.splice(this.bindings.indexOf(match[0]), 1);
      }
    };

    /**
     * Fetch a template configuration on-demand. Send the action 
     * 'template-config' to the node backend. 
     */
    DataSource.prototype.config = function (resource, cb) {
      var deferred = $q.defer();
   
      var id = generateUUID();
      resource.id = id;
      this.bindings.push({
        resource: resource,
        id: id,
        callback: function (result) {
          cb && cb.apply(this, arguments);
          deferred.resolve(result);
        },
        errorCallback: function(err) {
          deferred.reject(err);
        }
      });
      mySocket.send({
        action: 'template-config',
        resource: resource
      })
      return deferred.promise;
    }

    /**
     * Fetch a resource on-demand. Send the action 'request' to
     * the node backend.
     */
    DataSource.prototype.request = function (resource, cb) {
      var deferred = $q.defer();

      var id = generateUUID();
      resource.id = id;
      this.bindings.push({
        resource: resource,
        id: id,
        callback: function (result) {
          /*jshint -W030 */
          cb && cb.apply(this, arguments);
          deferred.resolve(result);
        },
        errorCallback: function (err) {
          deferred.reject(err);
        }
      });

      mySocket.send({
        action: 'request',
        resource: resource
      });

      return deferred.promise;
    };

    return DataSource;
  });
