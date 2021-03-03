/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

import sockjs from 'sockjs';
import http from 'http';
import fs from 'fs';
import log4js from 'log4js';
import https from 'https';
import ip from 'ip';
import { getApp } from 'server/express';
import Aggregator from 'server/aggregator';
import { extractConfig } from 'server/config/parser';
import { getCDAPConfig } from 'server/cdap-config';
import { applyGraphQLMiddleware } from 'gql/graphql';
import { getHostName } from 'server/config/hostname';
import middleware404 from 'server/middleware-404';

var cdapConfig,
  securityConfig,
  allowedOrigin = [],
  wsConnections = {},
  hostname,
  hostIP = ip.address();

/**
 * Configuring the logger. In order to use the logger anywhere
 * in the BE, include the following:
 *    var log4js = require('log4js');
 *    var logger = log4js.getLogger();
 *
 * We configure using LOG4JS_CONFIG specified file or we use
 * the default provided in the conf/log4js.json file.
 */
if (!process.env.LOG4JS_CONFIG) {
  log4js.configure(__dirname + '/server/config/log4js.json');
}

// Get a log handle.
var log = log4js.getLogger('default');

function getFullURL(host) {
  let nodejsport = cdapConfig['dashboard.bind.port'];
  const isSSLEnabled = cdapConfig['ssl.external.enabled'] === 'true';
  const nodejsprotocol = isSSLEnabled ? 'https' : 'http';
  if (isSSLEnabled) {
    nodejsport = cdapConfig['dashboard.ssl.bind.port'];
  }
  let baseUrl = `${nodejsprotocol}://${host}`;
  return nodejsport ? `${baseUrl}:${nodejsport}` : baseUrl;
}
async function setAllowedOrigin() {
  const nodejsserver = cdapConfig['dashboard.bind.address'];

  // protocol and port will be added to this domain
  const whitelistedDomain = cdapConfig['dashboard.domain.name'];

  // take exact domain as-is from config
  const whitelistedOrigin = cdapConfig['dashboard.origin'];
  allowedOrigin = [getFullURL(nodejsserver)];
  try {
    hostname = await getHostName();
  } catch (e) {
    log.error('Unable to determine hostname: ' + e);
    hostname = null;
  }
  if (hostname) {
    allowedOrigin.push(getFullURL(hostname));
  }
  if (hostIP) {
    allowedOrigin.push(getFullURL(hostIP));
  }
  if (whitelistedDomain) {
    allowedOrigin.push(getFullURL(whitelistedDomain));
  }
  if (['localhost', '127.0.0.1', '0.0.0.0'].indexOf(nodejsserver) !== -1) {
    allowedOrigin.push(getFullURL('127.0.0.1'), getFullURL('0.0.0.0'), getFullURL('localhost'));
  }
  if (whitelistedOrigin) {
    allowedOrigin.push(whitelistedOrigin);
  }
}

log.info('Starting CDAP UI ...');
getCDAPConfig()
  .then(function(c) {
    cdapConfig = c;
    if (cdapConfig['security.enabled'] === 'true') {
      log.debug('CDAP Security has been enabled');
      return extractConfig('security');
    }
  })

  .then(function(s) {
    securityConfig = s;
    setAllowedOrigin();
    return getApp(Object.assign({}, cdapConfig, securityConfig));
  })

  .then(function(app) {
    // handles /graphql route
    applyGraphQLMiddleware(app, Object.assign({}, cdapConfig, securityConfig), log);
    // handles all unmatched routes
    app.use(middleware404.render404);

    var port, server;
    if (cdapConfig['ssl.external.enabled'] === 'true') {
      if (cdapConfig['dashboard.ssl.disable.cert.check'] === 'true') {
        // For self signed certs: see https://github.com/mikeal/request/issues/418
        process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
      }

      try {
        server = https.createServer(
          {
            key: fs.readFileSync(securityConfig['dashboard.ssl.key']),
            cert: fs.readFileSync(securityConfig['dashboard.ssl.cert']),
          },
          app
        );
      } catch (e) {
        log.debug(
          'SSL key/cert files read failed. Please fix the key/ssl certificate files and restart node server -  ',
          e
        );
      }
      port = cdapConfig['dashboard.ssl.bind.port'];
    } else {
      server = http.createServer(app);
      port = cdapConfig['dashboard.bind.port'];
    }
    server.listen(port, cdapConfig['dashboard.bind.address'], function() {
      log.info('CDAP UI listening on port %s', port);
    });

    return server;
  })

  .then(async function(server) {
    var sockServer = sockjs.createServer({
      log: function(lvl, msg) {
        log.trace(msg);
      },
    });
    /**
     * Node server now supports Proxy mode. This means, between the client and the node proxy
     * there can be another proxy that handle authentication and pass on the user id and auth token.
     * This means the client will not know anything about the user but the node proxy and the backend
     * will be configured to pass on the auth token and user id from the proxy for authentication.
     *
     * This is the journey of an auth token and user id in proxy mode.
     *
     * 1. CDAP starts in k8s which spins up UI in a pod with
     *    security.authentication.mode: PROXY
     *    security.authentication.proxy.user.identity.header: x-inverting-proxy-user-id
     * 2. Once node proxy goes to PROXY mode, it will get the auth token only for the http
     *    requests.
     * 3. The client will not know about the auth token either.
     * 4. Once the client reaches CDAP UI, the proxy would have already authenticated the user.
     * 5. The request to upgrade websocket connection should already have the auth token and the user id
     * 6. We take those values and add to the connection object (sockjs connection object)
     * 7. This then gets picked up at the aggregator module that actually makes the call to the
     *    backend along with these in the request header.
     * 8. Upon receiving the response, we remove these from the request object and send it back
     *    to the client as if no authentication exists.
     */
    let authToken, userid;
    sockServer.on('connection', function(c) {
      log.debug('[SOCKET OPEN] Connection to client "' + c.id + '" opened');
      // @ts-ignore
      var a = new Aggregator(c, { ...cdapConfig, ...securityConfig });
      if (cdapConfig['security.authentication.mode'] === 'PROXY') {
        c.authToken = authToken;
        c.userid = userid;
      }
      wsConnections[c.id] = c;
      c.on('close', function() {
        log.debug('Cleaning out aggregator: ' + JSON.stringify(a.connection.id));
        a = null;
        c.end();
        c.destroy();
        delete wsConnections[c.id];
      });
    });

    sockServer.installHandlers(server, { prefix: '/_sock' });
    server.addListener('upgrade', function(req, socket) {
      if (cdapConfig['security.authentication.mode'] === 'PROXY') {
        authToken = req.headers.authorization;
        const userIdProperty = cdapConfig['security.authentication.proxy.user.identity.header'];
        userid = req.headers[userIdProperty];
      }
      if (allowedOrigin.indexOf(req.headers.origin) === -1) {
        log.info('Unknown Origin: ' + req.headers.origin);
        log.info('Denying socket connection and closing the channel');
        socket.end();
        socket.destroy();
        return;
      }
    });
    function gracefulShutdown() {
      log.info('Caught SIGTERM. Closing http & ws server');
      server.close();
      if(typeof wsConnections === 'object' && Object.keys(wsConnections).length) {
        log.debug(`Closing ${Object.keys(wsConnections).length} open websocket connections`)
        Object.values(wsConnections).forEach((connection) => {
          log.debug('Ending and destroying all graceful shutdown: ' + connection.readyState);
          connection.end();
          connection.destroy();
        });
        log.debug('Closed all open websocket connections');
        wsConnections = {};
      }
      process.exit(0);
    }
    process.on('SIGTERM', gracefulShutdown);
  });
