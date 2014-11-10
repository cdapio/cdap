
angular.module(PKG.name+'.services')

  .service('mySocket', function (mySocketFactory) {

    var sock = mySocketFactory({
      url: '/_sock'
    });

    sock
      .setHandler('open', function(){
        console.log('mySocket open', arguments);
      })
      .setHandler('message', function(){
        console.log('mySocket message', arguments);
      });

    return sock;
  })



  // inspired by angular-sockjs
  .factory('mySocketFactory', function ($timeout) {

      var asyncAngularify = function (socket, callback) {
        return callback ? function () {
          var args = arguments;
          $timeout(function () {
            callback.apply(socket, args);
          }, 0);
        } : angular.noop;
      };

      return function socketFactory (options) {
        options = options || {};
        var socket = options.socket || new window.SockJS(options.url);

        var wrappedSocket = {
          setHandler: function (event, callback) {
            socket['on' + event] = asyncAngularify(socket, callback);
            return this;
          },
          removeHandler: function(event) {
            delete socket['on' + event];
            return this;
          },
          send: function () {
            return socket.send.apply(socket, arguments);
          },
          close: function () {
            return socket.close.apply(socket, arguments);
          }
        };

        return wrappedSocket;
      };

  });

