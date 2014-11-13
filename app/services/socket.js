angular.module(PKG.name+'.services')

  .constant('MYSOCKET_EVENT', {
    message: 'mysocket-message',
    closed: 'mysocket-closed',
  })

  /*
    MyDataSource // usage in a controler:

    var dataSrc = new MyDataSource($scope);
    $scope.foo = {};
    dataSrc.poll('foo.bar', {method:'GET', path: '/whatever'});

   */
  .factory('MyDataSource', function (mySocket, MYSOCKET_EVENT) {

    var bindings = {};

    function DataSource (scope) {
      this.scope = scope;
      var id = scope.$id;

      if(bindings[id]) {
        throw new Error('multiple DataSource for scope', id);
      }
      bindings[id] = {};

      scope.$on(MYSOCKET_EVENT.message, function (event, data) {
        scope.$apply(function(){
          angular.forEach(bindings[id], function (val, key) {
            if(angular.equals(val, data.resource)) {
              var z = data.json || data.response;
              if(key.indexOf('.')===-1) {
                scope[key] = z;
              }
              else { // slower version supports nested keys
                scope.$eval(key+' = '+JSON.stringify(z));
              }
            }
          });
        });
      });

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
          console.info('[mySocket] opened');
          angular.forEach(buffer, send);
          buffer = [];
          attempt = 1;
        };

        socket.onclose = function (event) {
          console.error(event.reason);
          $rootScope.$broadcast(MYSOCKET_EVENT.closed, event);

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