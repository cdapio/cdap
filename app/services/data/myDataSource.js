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
  .factory('MyDataSource', function ($state, $log, $rootScope, caskWindowManager, mySocket,
    MYSOCKET_EVENT) {

    var instances = {}; // keyed by scopeid

    function _pollStart (resource) {
      mySocket.send({
        action: 'poll-start',
        resource: resource
      });
    }

    function _pollStop (resource) {
      mySocket.send({
        action: 'poll-stop',
        resource: resource
      });
    }

    function DataSource (scope) {
      var id = scope.$id,
          self = this;

      if(instances[id]) {
        throw new Error('multiple DataSource for scope', id);
      }
      instances[id] = self;

      this.bindings = [];

      scope.$on(MYSOCKET_EVENT.message, function (event, data) {
        if(data.statusCode!==200 || data.warning) {
          return; // errors are handled at $rootScope level
        }
        angular.forEach(self.bindings, function (b) {
          if(angular.equals(b.resource, data.resource)) {
            scope.$apply(b.callback.bind(null, data.response));
          }
        });
      });

      scope.$on(MYSOCKET_EVENT.reconnected, function () {
        $log.log('[DataSource] reconnected, reloading...');

        // https://github.com/angular-ui/ui-router/issues/582
        $state.transitionTo($state.current, $state.$current.params,
          { reload: true, inherit: true, notify: true }
        );
      });

      scope.$on('$destroy', function () {
        delete instances[id];
      });

      scope.$on(caskWindowManager.event.blur, function () {
        angular.forEach(self.bindings, function (b) {
          _pollStop(b.resource);
        });
      });

      scope.$on(caskWindowManager.event.focus, function () {
        angular.forEach(self.bindings, function (b) {
          _pollStart(b.resource);
        });
      });

      this.scope = scope;
    }

    DataSource.prototype.poll = function (resource, cb) {
      this.bindings.push({
        resource: resource,
        callback: cb
      });

      this.scope.$on('$destroy', function () {
        _pollStop(resource);
      });

      _pollStart(resource);
    };

    DataSource.prototype.request = function (resource, cb) {
      var once = false;

      this.bindings.push({
        resource: resource,
        callback: function() {
          if(!once) {
            once = true;
            cb.apply(this, arguments);
          }
        }
      });

      mySocket.send({
        action: 'request',
        resource: resource
      });
    };

    return DataSource;
  });
