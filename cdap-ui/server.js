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
    sockjs = require('sockjs'),
    http = require('http'),
    fs = require('fs'),
    log4js = require('log4js'),
    https = require('https');

var cdapConfig, securityConfig;

/**
 * Configuring the logger. In order to use the logger anywhere
 * in the BE, include the following:
 *    var log4js = require('log4js');
 *    var logger = log4js.getLogger();
 *
 * We configure using LOG4JS_CONFIG specified file or we use
 * the default provided in the conf/log4js.json file.
 */
if(process.env.LOG4JS_CONFIG) {
  log4js.configure({}, { reloadSecs: 600});
} else {
  log4js.configure(__dirname + "/server/config/log4js.json", { reloadSecs: 300});
}

// Get a log handle.
var log = log4js.getLogger('default');

log.info("Starting CDAP UI ...");
parser.extractConfig('cdap')

  .then(function (c) {
    cdapConfig = c;
    if (cdapConfig['ssl.enabled'] === 'true') {
      log.debug("CDAP Security has been enabled");
      return parser.extractConfig('security');
    }
  })

  .then(function (s) {
    securityConfig = s;
    return express.getApp(cdapConfig);
  })

  .then(function (app) {
    var port, server;

    if (cdapConfig['ssl.enabled'] === 'true') {

      if (cdapConfig['dashboard.ssl.disable.cert.check'] === 'true') {
        // For self signed certs: see https://github.com/mikeal/request/issues/418
        process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
      }

      try {
        server = https.createServer({
          key: fs.readFileSync(securityConfig['dashboard.ssl.key']),
          cert: fs.readFileSync(securityConfig['dashboard.ssl.cert'])
        }, app);
      } catch(e) {
        log.debug('SSL key/cert files read failed. Please fix the key/ssl certificate files and restart node server -  ', e);
      }
      port = cdapConfig['dashboard.ssl.bind.port'];
    }
    else {
      server = http.createServer(app);
      port = cdapConfig['dashboard.bind.port'];
      // port = 8080; // so we can have old UI running while developing
    }

    server.listen(port, cdapConfig['dashboard.bind.address'], function () {
      log.info('CDAP UI listening on port %s', port);
    });

    return server;
  })

  .then(function (server) {

    var sockServer = sockjs.createServer({
      log: function (lvl, msg) {
        log.trace(msg);
      }
    });

    sockServer.on('connection', function (c) {
      var a = new Aggregator(c);
      c.on('close', function () {
        delete a;
      });
    });

    sockServer.installHandlers(server, { prefix: '/_sock' });
  });
