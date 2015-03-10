/**
 * Spins up servers
 */
var express = require('./server/express.js'),
    Aggregator = require('./server/aggregator.js'),
    parser = require('./server/config/parser.js'),
    sockjs = require('sockjs'),
    colors = require('colors/safe'),
    http = require('http'),
    https = require('https');


var cdapConfig, securityConfig;

parser.extractConfig('cdap')

  .then(function (c) {
    cdapConfig = c;
    if (cdapConfig['ssl.enabled'] === 'true') {
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

      server = https.createServer({
        key: fs.readFileSync(securityConfig['dashboard.ssl.cert']),
        cert: fs.readFileSync(securityConfig['dashboard.ssl.key'])
      }, app);
      port = cdapConfig['dashboard.ssl.bind.port'];
    }
    else {
      server = http.createServer(app);
      // port = cdapConfig['dashboard.bind.port'];
      port = 8080; // so we can have old UI running while developing
    }

    server.listen(port, cdapConfig['dashboard.bind.address'], function () {
        console.info(colors.yellow('http')+' listening on port %s', port);
    });

    return server;
  })

  .then(function (server) {

    // sockjs
    var sockServer = sockjs.createServer({
      log: function (lvl, msg) {
        // console.log(colors.blue('sock'), msg);
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
