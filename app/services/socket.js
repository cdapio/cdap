angular.module(PKG.name+'.services')

  .constant('MYSOCKET_EVENT', {
    message: 'mysocket-message',
    closed: 'mysocket-closed',
  })

  /*
    MyDataSource

    // usage in a controler:

    var dataSrc = new MyDataSource($scope);
    $scope.foo = [];
    dataSrc.poll('foo', {method:'GET', path: '/whatever'});

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
              scope.$eval(key+' = '+JSON.stringify(data.response));
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

      var socket = new window.SockJS(this.prefix),
          buffer = [];

      socket.onmessage = function (event) {
        try {
          var data = JSON.parse(event.data);
          $rootScope.$broadcast(MYSOCKET_EVENT.message, data);
        }
        catch(e) {
          console.error(e);
        }
      };

      socket.onopen = function () {
        angular.forEach(buffer, send);
        buffer = [];
      };

      socket.onclose = function () {
        $rootScope.$broadcast(MYSOCKET_EVENT.closed, arguments);
      };

      function send(obj) {
        if(!socket.readyState) {
          buffer.push(obj);
          return false;
        }
        socket.send(JSON.stringify(angular.extend({
          user: myAuth.currentUser
        }, obj)));
        return true;
      }

      return {
        send: send,
        close: function () {
          return socket.close.apply(socket, arguments);
        }
      };
    };

  })

  ;