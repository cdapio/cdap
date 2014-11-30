/**
 * Spins up servers
 */
var app = require('./server/express.js'),
    configParser = require('./server/configParser.js'),
    Aggregator = require('./server/aggregator.js')
    sockjs = require('sockjs'),
    colors = require('colors/safe'),
    http = require('http'),
    PORT = 8080,
    mode = process.env.CDAP_MODE || null,
    httpServer = http.createServer(app);


configParser.extractConfig(mode, 'cdap', false /* isSecure*/)
  .then(function(config) {
    // http
    httpServer.listen(PORT, config['router.bind.address'], function () {
      console.info(colors.yellow('http')+' listening on port %s', PORT);
    });

    // sockjs
    var sockServer = sockjs.createServer({
      log: function(lvl, msg) {
        console.log(colors.blue('sock'), msg);
      }
    });

    sockServer.on('connection', function(c) {
      var a = new Aggregator(c);
      c.on('close', function () {
        delete a;
      });
    });

    sockServer.installHandlers(httpServer, { prefix: '/_sock' });

  });
