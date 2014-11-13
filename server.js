/**
 * Spins up servers
 */
var app = require('./server/express.js'),
    Aggregator = require('./server/aggregator.js')
    sockjs = require('sockjs'),
    colors = require('colors/safe'),
    http = require('http'),

    PORT = 8080,
    httpServer = http.createServer(app);


// http
httpServer.listen(PORT, '0.0.0.0', function () {
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
