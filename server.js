/**
 * Spins up servers
 */
var appMaker = require('./server/express.js'),
    Aggregator = require('./server/aggregator.js'),
    parser = require('./server/config/parser.js'),
    sockjs = require('sockjs'),
    colors = require('colors/safe'),
    http = require('http'),
    https = require('https'),
    PORT = 8080;


var cdapConfig;

parser.extractConfig('cdap')

  .then(function (c) {
    cdapConfig = c;
    return appMaker.promise(c);
  })

  .then(function (app) {
    var httpBackend = http;

    if (cdapConfig['ssl.enabled'] === 'true') {
      httpBackend = https;
    }

    // http
    httpBackend.createServer(app).listen(PORT, cdapConfig['router.bind.address'], function () {
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
