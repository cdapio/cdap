/**
 * Spins up web server
 */

var pkg = require('./package.json'),
    morgan = require('morgan'),
    express = require('express'),
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

var app = express(),
    httpLabel = color.green('http'),
    corsLabel = color.pink('cors'),
    httpStaticLogger = morgan(httpLabel+' :method :url :status'),
    httpIndexLogger = morgan(httpLabel+' :method '+color.hilite(':url')+' :status');

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

app.listen(PORT, '0.0.0.0', function () {
  console.info(httpLabel+' listening on port %s', PORT);
});
