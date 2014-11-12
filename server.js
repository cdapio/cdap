/**
 * Spins up web server
 */

var pkg = require('./package.json'),
    morgan = require('morgan'),
    express = require('express'),
    sockjs = require('sockjs'),
    http = require('http'),
    finalhandler = require('finalhandler'),
    serveFavicon = require('serve-favicon'),

    PORT = 8080,

    colors = require('colors/safe');


morgan.token('cooprcred', function(req, res){
  return colors.cyan(req.headers['coopr-userid'] + '/' + req.headers['coopr-tenantid']);
});

morgan.token('ms', function(req, res){
  if (!res._header || !req._startAt) return '';
  var diff = process.hrtime(req._startAt);
  var ms = diff[0] * 1e3 + diff[1] * 1e-6;
  return Math.ceil(ms)+'ms';
});

var app = express(),
    httpStaticLogger = morgan(colors.green('http')+' :method :url :ms :status'),
    httpIndexLogger = morgan(colors.inverse('http')+' :method :url :ms :status');

console.log(colors.underline(pkg.name) + ' v' + pkg.version + ' starting up...');

try { app.use(serveFavicon(__dirname + '/dist/assets/img/favicon.png')); }
catch(e) { console.error("Favicon missing! Did you run `gulp build`?"); }

// serve the config file
app.get('/config.js', function (req, res) {
  var data = JSON.stringify({
    // the following will be available in angular via the "MY_CONFIG" injectable

    authorization: req.headers.authorization

  });
  res.header({
    'Content-Type': 'text/javascript',
    'Cache-Control': 'no-store, must-revalidate'
  });
  res.send('angular.module("'+pkg.name+'.config", [])' +
            '.constant("MY_CONFIG",'+data+');');
});

// serve static assets
app.use('/assets', [
  httpStaticLogger,
  express.static(__dirname + '/dist/assets', {
    index: false
  }),
  function(req, res) {
    finalhandler(req, res)(false); // 404
  }
]);

app.get('/robots.txt', [
  httpStaticLogger,
  function (req, res) {
    res.type('text/plain');
    res.send('User-agent: *\nDisallow: /');
  }
]);


// any other path, serve index.html
app.all('*', [
  httpIndexLogger,
  function (req, res) {
    res.sendFile(__dirname + '/dist/index.html');
  }
]);


var httpServer = http.createServer(app);

httpServer.listen(PORT, '0.0.0.0', function () {
  console.info(colors.yellow('http')+' listening on port %s', PORT);
});


// sockjs
var sockServer = sockjs.createServer({
  log: function(lvl, msg) {
    console.log(colors.blue('sock'), msg);
  }
});

var Dispatch = require('./socket-dispatch.js');

sockServer.on('connection', Dispatch);

sockServer.installHandlers(httpServer, { prefix: '/_sock' });
