angular.module(PKG.name+'.services')
  /*
    MyDataSource // usage in a controler:

    var dataSrc = new MyDataSource($scope);

    // polling a namespaced resource example:
    dataSrc.poll({
        method: 'GET',
        _cdapNsPath: '/foo/bar'
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
  .factory('MyDataSource', function ($log, $rootScope, caskWindowManager, mySocket,
    MYSOCKET_EVENT, $q, MyPromise, $timeout) {

    var instances = {}; // keyed by scopeid

    function _pollStart (resource) {
      var re = {};

      if (!resource.url) {
        re = resource;
      } else {
        re = {
          url: resource.url,
          json: true,
          method: resource.method
        };
      }

      mySocket.send({
        action: 'poll-start',
        resource: re
      });
    }

    function _pollStop (resource) {

      var re = {};
      if (!resource.url) {
        re = resource;
      }else {
        re = {
          url: resource.url,
          json: true,
          method: resource.method
        };
      }
      mySocket.send({
        action: 'poll-stop',
        resource: re
      });
    }

    $rootScope.$on(MYSOCKET_EVENT.reconnected, function () {
      // $log.log('[DataSource] reconnected, reloading...');

      // https://github.com/angular-ui/ui-router/issues/582
      // $state.transitionTo($state.current, $state.$current.params,
      //   { reload: true, inherit: true, notify: true }
      // );
      window.$go('home');
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

      $rootScope.$on(MYSOCKET_EVENT.message, function (event, data) {
        if(data.statusCode>299 || data.warning) {
          angular.forEach(self.bindings, function (b) {
            if(angular.equals(b.resource, data.resource)) {
              if(b.errorCallback) {
                $rootScope.$applyAsync(b.errorCallback.bind(null, {data: data.response}));
              }
            }
          });
          return; // errors are handled at $rootScope level
        }
        angular.forEach(self.bindings, function (b) {
          if(angular.equals(b.resource, data.resource)) {
            if (angular.isFunction(b.callback)) {
              $rootScope.$applyAsync(b.callback.bind(null, data.response));
            }

            if (b && b.resolve) {
              b.resolve({data: data.response});
            }
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
     * poll a resource
     */
    DataSource.prototype.poll = function (resource, cb) {
      var self = this;
      var prom = new MyPromise(function(resolve, reject) {
        var re = {};
        if (!resource.url) {
          re = resource;
        }else {
          re = {
            url: resource.url,
            json: true,
            method: resource.method
          };
        }

        var a = {
          poll: true,
          resource: re,
          callback: cb,
          resolve: resolve,
          reject: reject
        };


        self.bindings.push(a);

        self.scope.$on('$destroy', function () {
          _pollStop(resource);
        });

        _pollStart(resource);
      }, true);
      return prom;
    };

    DataSource.prototype.pollStop = function(resource) {
      var prom = new MyPromise(function(resolve, reject) {
        _pollStop(resource);
      });
      return prom;
    };


    /**
     * fetch a resource
     */
    DataSource.prototype.request = function (resource, cb) {
      var once = false,
          self = this;
      var prom = new MyPromise(function(resolve, reject) {
          self.bindings.push({
            resource: resource,
            callback: cb,
            resolve: resolve,
            reject: reject
          });

          // $timeout(function() {
            mySocket.send({
              action: 'request',
              resource: resource
            });
          //});

      }, false);
      prom = prom.then(function(res) {
        res = res.data;
        return res;
      });
      return prom;
    };

    return DataSource;
  });
