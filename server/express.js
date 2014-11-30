/*global require, module, process, __dirname */

/**
 * Express app
 */

var pkg = require('../package.json'),
    morgan = require('morgan'),
    express = require('express'),
    finalhandler = require('finalhandler'),
    serveFavicon = require('serve-favicon'),
    colors = require('colors/safe'),
    mode = process.env.CDAP_MODE || null,
    configParser = require('./configParser.js'),
    config = {},
    DIST_PATH = require('path').normalize(
      __dirname + '/../dist'
    );

configParser.extractConfig(mode, 'cdap', false /* isSecure*/)
  .then(function(c) {
    config = c;
  })

morgan.token('ms', function(req, res){
  if (!res._header || !req._startAt) { return ''; }
  var diff = process.hrtime(req._startAt);
  var ms = diff[0] * 1e3 + diff[1] * 1e-6;
  return Math.ceil(ms)+'ms';
});

var app = express(),
    httpStaticLogger = morgan(colors.green('http')+' :method :url :ms :status'),
    httpIndexLogger = morgan(colors.inverse('http')+' :method :url :ms :status');

console.log(colors.underline(pkg.name) + ' v' + pkg.version + ' starting up...');

try { app.use(serveFavicon(DIST_PATH + '/assets/img/favicon.png')); }
catch(e) { console.error('Favicon missing! Did you run `gulp build`?'); }

// serve the config file
app.get('/config.js', function (req, res) {
  var data = JSON.stringify({
    // the following will be available in angular via the "MY_CONFIG" injectable

    authorization: req.headers.authorization,
    routerServerUrl: config['router.server.address'],
    routerServerPort: config['router.server.port']
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
  express.static(DIST_PATH + '/assets', {
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
    res.sendFile(DIST_PATH + '/index.html');
  }
]);

module.exports = app;
