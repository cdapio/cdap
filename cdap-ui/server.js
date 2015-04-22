/**
 * Spins up servers
 */
var express = require('./server/express.js'),
    Aggregator = require('./server/aggregator.js'),
    parser = require('./server/config/parser.js'),
    sockjs = require('sockjs'),
    http = require('http'),
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

      server = https.createServer({
        key: fs.readFileSync(securityConfig['dashboard.ssl.cert']),
        cert: fs.readFileSync(securityConfig['dashboard.ssl.key'])
      }, app);
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
