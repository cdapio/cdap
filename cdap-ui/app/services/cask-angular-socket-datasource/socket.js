/*
 * Copyright © 2015-2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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

  this.$get = function (MYSOCKET_EVENT, SockJS, $log, EventPipe) {

    var self = this,
        socket = null,
        buffer = [],
        firstTime = true;

    function init (attempt) {
      $log.log('[mySocket] init');

      attempt = attempt || 1;
      socket = new SockJS(self.prefix);

      socket.onmessage = function (event) {
        try {
          var data = JSON.parse(event.data);
          $log.debug('[mySocket] ←', data);
          EventPipe.emit(MYSOCKET_EVENT.message, data);
        }
        catch(e) {
          $log.error(e);
        }
      };

      socket.onopen = function () {
        if (!firstTime) {
          window.CaskCommon.SessionTokenStore.fetchSessionToken().then(() => {
            EventPipe.emit(MYSOCKET_EVENT.reconnected);
            attempt = 1;
          }, () => {
            console.log('Failed to fetch session token');
          });
        }
        firstTime = false;

        $log.info('[mySocket] opened');
        angular.forEach(buffer, send);
        buffer = [];
      };

      socket.onclose = function (event) {
        $log.error(event.reason);
        EventPipe.emit('backendDown', 'User interface service is down');

        if(attempt<2) {
          EventPipe.emit(MYSOCKET_EVENT.closed, event);
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

      doSend(obj);

      return true;
    }

    function doSend(obj) {
      var msg = obj,
          r = obj.resource;

      if(r) {
        msg.resource = r;

        // Majority of the time, we send data as json and expect a json response, but not always (i.e. stream ingest).
        // Default to json content-type.
        if (msg.resource.json === undefined) {
          msg.resource.json = true;
        }

        if (!r.method) {
          msg.resource.method = 'GET';
        }

        $log.debug('[mySocket] →', msg.action, r.method, r.url);
      }
      msg.sessionToken = window.CaskCommon.SessionTokenStore.default.getState();

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
