/*
 * Copyright © 2015 Cask Data, Inc.
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

/**
 * Spins up servers
 */
var express = require('./server/express.js'),
  Aggregator = require('./server/aggregator.js'),
  parser = require('./server/config/parser.js'),
  cdapConfigurator = require('./server/cdap-config.js'),
  sockjs = require('sockjs'),
  http = require('http'),
  fs = require('fs'),
  log4js = require('log4js'),
  graphql = require('./graphql/graphql.js'),
  https = require('https'),
  getHostName = require('./server/config/hostname'),
  ip = require('ip');

var cdapConfig,
  securityConfig,
  allowedOrigin = [],
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
cdapConfigurator
  .getCDAPConfig()
  .then(function(c) {
    cdapConfig = c;
    if (cdapConfig['security.enabled'] === 'true') {
      log.debug('CDAP Security has been enabled');
      return parser.extractConfig('security');
    }
  })

  .then(function(s) {
    securityConfig = s;
    setAllowedOrigin();
    return express.getApp(Object.assign({}, cdapConfig, securityConfig));
  })

  .then(function(app) {
    graphql.applyMiddleware(app, Object.assign({}, cdapConfig, securityConfig), log);

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

    sockServer.on('connection', function(c) {
      // @ts-ignore
      var a = new Aggregator(c);
      c.on('close', function() {
        a = null;
      });
    });

    sockServer.installHandlers(server, { prefix: '/_sock' });
    server.addListener('upgrade', function(req, socket) {
      if (allowedOrigin.indexOf(req.headers.origin) === -1) {
        log.info('Unknown Origin: ' + req.headers.origin);
        log.info('Denying socket connection and closing the channel');
        socket.end();
        socket.destroy();
      }
    });
  });
