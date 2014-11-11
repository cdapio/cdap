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

    color = {
      hilite: function (v) { return '\x1B[7m' + v + '\x1B[27m'; },
      green: function (v) { return '\x1B[40m\x1B[32m' + v + '\x1B[39m\x1B[49m'; },
      pink: function (v) { return '\x1B[40m\x1B[35m' + v + '\x1B[39m\x1B[49m'; }
    };


morgan.token('cooprcred', function(req, res){
  return color.pink(req.headers['coopr-userid'] + '/' + req.headers['coopr-tenantid']);
});

morgan.token('ms', function(req, res){
  if (!res._header || !req._startAt) return '';
  var diff = process.hrtime(req._startAt);
  var ms = diff[0] * 1e3 + diff[1] * 1e-6;
  return Math.ceil(ms)+'ms';
});

var app = express(),
    httpLabel = color.green('http'),
    httpStaticLogger = morgan(httpLabel+' :method '+color.green(':url')+' :ms :status'),
    httpIndexLogger = morgan(httpLabel+' :method '+color.hilite(':url')+' :ms :status');

console.log(color.hilite(pkg.name) + ' v' + pkg.version + ' starting up...');

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
  console.info(httpLabel+' listening on port %s', PORT);
});


// sockjs
var sockServer = sockjs.createServer({
  log: function(lvl, msg) {
    console.log(color.green('sock'), msg);
  }
});

sockServer.on('connection', function (conn) {
  conn.on('data', function (message) {
    message = JSON.parse(message);

    console.log(color.pink('sock'), conn.id, message);

    conn.write(JSON.stringify({
      response: [Date.now(),2,0],
      resource: message.resource
    }));
  });
});

sockServer.installHandlers(httpServer, { prefix: '/_sock' });
