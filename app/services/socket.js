angular.module(PKG.name+'.services')

  .constant('MYSOCKET_EVENT', {
    message: 'mysocket-message',
    closed: 'mysocket-closed',
    reconnected: 'mysocket-reconnected'
  })

  /*
    MyDataSource // usage in a controler:

    var dataSrc = new MyDataSource($scope);
    $scope.foo = {};
    dataSrc.poll('foo.bar', {method:'GET', path: '/whatever'});

   */
  .factory('MyDataSource', function ($state, mySocket, MYSOCKET_EVENT) {

    var bindings = {};

    function DataSource (scope) {
      var id = scope.$id;

      if(bindings[id]) {
        throw new Error('multiple DataSource for scope', id);
      }
      bindings[id] = {};

      scope.$on(MYSOCKET_EVENT.message, function (event, data) {
        angular.forEach(bindings[id], function (val, key) {
          if(angular.equals(val, data.resource)) {
            var z = data.json || data.response;
            if(key.indexOf('.')===-1) {
              scope.$apply(function(){
                scope[key] = z;
              });
            }
            else { // slower version supports nested keys
              scope.$eval(key+' = '+JSON.stringify(z));
            }
          }
        });
      });

      scope.$on(MYSOCKET_EVENT.reconnected, function (event, data) {
        console.log('[DataSource] reconnected, reloading...');

        // https://github.com/angular-ui/ui-router/issues/582
        $state.transitionTo($state.current, $state.$current.params,
          { reload: true, inherit: true, notify: true }
        );
      });

      scope.$on('$destroy', function () {
        delete bindings[id];
      });

      this.scope = scope;
    }


    DataSource.prototype.fetch = function (key, resourceObj) {
      bindings[this.scope.$id][key] = resourceObj;

      mySocket.send({
        action: 'fetch',
        resource: resourceObj
      });
    };


    DataSource.prototype.poll = function (key, resourceObj) {
      bindings[this.scope.$id][key] = resourceObj;

      mySocket.send({
        action: 'poll-start',
        resource: resourceObj
      });

      this.scope.$on('$destroy', function () {
        mySocket.send({
          action: 'poll-stop',
          resource: resourceObj
        });
      });
    };

    return DataSource;
  })




  .provider('mySocket', function () {

    this.prefix = '/_sock';

    this.$get = function (MYSOCKET_EVENT, myAuth, $rootScope) {

      var self = this,
          socket = null,
          buffer = [];

      function init (attempt) {
        console.log('[mySocket] init');

        attempt = attempt || 1;
        socket = new window.SockJS(self.prefix);

        socket.onmessage = function (event) {
          try {
            var data = JSON.parse(event.data);
            console.log('[mySocket] ←', data);
            $rootScope.$broadcast(MYSOCKET_EVENT.message, data);
          }
          catch(e) {
            console.error(e);
          }
        };

        socket.onopen = function (event) {

          if(attempt>1) {
            $rootScope.$broadcast(MYSOCKET_EVENT.reconnected, event);
            attempt = 1;
          }

          console.info('[mySocket] opened');
          angular.forEach(buffer, send);
          buffer = [];
        };

        socket.onclose = function (event) {
          console.error(event.reason);

          if(attempt<2) {
            $rootScope.$broadcast(MYSOCKET_EVENT.closed, event);
          }

          // reconnect with exponential backoff
          var d = Math.max(500, Math.round(
            (Math.random() + 1) * 500 * Math.pow(2, attempt)
          ));
          console.log('[mySocket] will try again in ',d+'ms');
          setTimeout(function () {
            init(attempt+1);
          }, d);
        };

      }

      function send(obj) {
        if(!socket.readyState) {
          buffer.push(obj);
          return false;
        }
        var msg = angular.extend({
          user: myAuth.currentUser
        }, obj);
        console.log('[mySocket] →', msg);
        socket.send(JSON.stringify(msg));
        return true;
      }

      init();

      return {
        init: init,
        send: send,
        close: function () {
          return socket.close.apply(socket, arguments);
        }
      };
    };

  })

  ;