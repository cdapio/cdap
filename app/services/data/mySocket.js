angular.module(PKG.name+'.services')

.factory('SockJS', function ($window) {
  return $window.SockJS;
})

.constant('MYSOCKET_EVENT', {
  message: 'mysocket-message',
  closed: 'mysocket-closed',
  reconnected: 'mysocket-reconnected'
})

.provider('mySocket', function () {

  this.prefix = '/_sock';

  this.$get = function (MYSOCKET_EVENT, myAuth, $rootScope, SockJS, $log, MY_CONFIG, myBaseUrl) {

    var self = this,
        socket = null,
        buffer = [];

    function init (attempt) {
      $log.log('[mySocket] init');

      attempt = attempt || 1;
      socket = new SockJS(self.prefix);

      socket.onmessage = function (event) {
        try {
          var data = JSON.parse(event.data);
          $log.log('[mySocket] ←', data);
          $rootScope.$broadcast(MYSOCKET_EVENT.message, data);
        }
        catch(e) {
          $log.error(e);
        }
      };

      socket.onopen = function (event) {

        if(attempt>1) {
          $rootScope.$broadcast(MYSOCKET_EVENT.reconnected, event);
          attempt = 1;
        }

        $log.info('[mySocket] opened');
        angular.forEach(buffer, send);
        buffer = [];
      };

      socket.onclose = function (event) {
        $log.error(event.reason);

        if(attempt<2) {
          $rootScope.$broadcast(MYSOCKET_EVENT.closed, event);
        }

        // reconnect with exponential backoff
        var d = Math.max(500, Math.round(
          (Math.random() + 1) * 500 * Math.pow(2, attempt)
        ));
        $log.log('[mySocket] will try again in ',d+'ms');
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

      doSend(obj, obj.namespace || null);

      return true;
    }

    function doSend(obj, namespace) {
      var msg = angular.extend({

            user: myAuth.currentUser

          }, obj),

          r = obj.resource;

      if(r) {
        msg.resource = r;
        // we only support json content-type,
        // and expect json as response
        msg.resource.json = true;

        // sugar for prefixing the path with namespace
        if(r._cdapNsPath) {
          r._cdapPath = [
            '/namespaces/',
            namespace.name,
            r._cdapNsPath
          ].join('');
          delete msg.resource._cdapNsPath;
        }

        // further sugar for building absolute url
        if(r._cdapPath) {
          msg.resource.url = [
            myBaseUrl,
            r._cdapPath
          ].join('');
          delete msg.resource._cdapPath;
        }

        if(MY_CONFIG.securityEnabled) {
          msg.resource.headers = angular.extend(r.headers || {}, {
            authorization: 'Bearer ' + myAuth.currentUser.token
          });
        }
      }

      $log.log('[mySocket] →', msg.action, r.method, r.url);
      socket.send(JSON.stringify(msg));
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

});
