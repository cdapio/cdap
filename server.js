/**
 * Spins up servers
 */
var AppServer = require('./server/appserver.js'),
    configParser = require('./server/config/configParser.js'),
    Aggregator = require('./server/aggregator.js')
    sockjs = require('sockjs'),
    colors = require('colors/safe'),
    PORT = 8080,
    mode = process.env.CDAP_MODE || null;


configParser.extractConfig(mode, 'cdap', false /* isSecure*/)
.then(function(config) { 
  new AppServer(config)
  .then(function (httpServer) {
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
  })
  .catch(function () {
    console.info('Exiting');
    process.exit(1);
  });
});