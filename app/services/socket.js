
angular.module(PKG.name+'.services')

  .constant('MY_SOCKET_EVENT', {
    message: 'my-socket-message'
  })

  .provider('mySocket', function () {

    this.prefix = '/_sock';

    this.$get = function (MY_SOCKET_EVENT, $rootScope) {
      var socket = new window.SockJS(this.prefix);

      socket.onmessage = function (event) {
        $rootScope.$broadcast(MY_SOCKET_EVENT.message, event.data);
      }

      return socket;
    };

  })

  ;