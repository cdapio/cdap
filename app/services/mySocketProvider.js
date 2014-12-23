angular.module(PKG.name+'.services')
.provider('mySocket', function () {

  this.prefix = '/_sock';

  this.$get = function (MYSOCKET_EVENT, myAuth, $rootScope, SockJS, $log, MY_CONFIG, myNamespaceMediator) {

    var self = this,
        socket = null,
        ns = null,
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

      if (obj.resource._cdapPath && !ns) {
        myNamespaceMediator.getCurrentNamespace()
          .then(function (namespace) {
            ns = namespace.name;
            doSend(obj);
          });
      } else {
        doSend(obj);
      }
      return true;
    }

    function doSend(obj) {
      var msg = angular.extend({

            user: myAuth.currentUser

          }, obj),
          r = obj.resource,
          baseUrl = 'http://' +
            MY_CONFIG.cdap.routerServerUrl +
            ':' +
            MY_CONFIG.cdap.routerServerPort +
            '/v3';

      if(r) {
        msg.resource = r;
        // we only support json content-type,
        // and expect json as response
        msg.resource.json = true;

        /*
          if _cdapPath is there -> construct url
          else use the entire resource object

          In this case it will be used like this,
          myDatasrc.request({
            _cdapPath: '/apps/myApp'
            method: 'GET'
          });
          or
          No _cdapPath --> use the entire resource object as it is.
          myDatasrc.request({
            path: <entire-url>,
            method: POST,
            body: {
              name: "asdad",
              description: "asdasd",
              displayName: "axcxc"
            }
          }, function() {
            ...
          })

        */
        if(r._cdapPath) {
          msg.resource.url = baseUrl +
            '/namespaces/' +
            ns +
            r._cdapPath;
          delete msg.resource._cdapPath;
        }

        if(MY_CONFIG.securityEnabled) {
          msg.resource.headers = angular.extend(r.headers || {}, {
            authorization: 'Bearer ' + myAuth.currentUser.token
          });
        }
      }

      $log.log('[mySocket] →', msg);
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

})

;
