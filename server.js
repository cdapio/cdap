/**
 * Spins up servers
 */
var appMaker = require('./server/express.js'),
    Aggregator = require('./server/aggregator.js'),
    configParser = require('./server/config/parser.js'),
    sockjs = require('sockjs'),
    colors = require('colors/safe'),
    http = require('http'),
    https = require('https'),
    PORT = 8080;


var configuration;

configParser.promise()

  .then(function (c) {
    configuration = c;
    return appMaker.promise(c);
  })

  .then(function (app) {
    var httpServer = http;

    if (configuration['ssl.enabled'] === 'true') {
      httpServer = https;
      if (configuration["dashboard.ssl.disable.cert.check"] === "true") {
        // For self signed certs: see https://github.com/mikeal/request/issues/418
        process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
      }
    }

    // http
    httpServer.listen(PORT, configuration['router.bind.address'], function () {
      console.info(colors.yellow('http')+' listening on port %s', PORT);
    });

    // sockjs
    var sockServer = sockjs.createServer({
      log: function (lvl, msg) {
        console.log(colors.blue('sock'), msg);
      }
    });

    sockServer.on('connection', function (c) {
      var a = new Aggregator(c);
      c.on('close', function () {
        delete a;
      });
    });

    sockServer.installHandlers(httpServer, { prefix: '/_sock' });
  });
