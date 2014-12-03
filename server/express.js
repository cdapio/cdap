/*global require, module, process, __dirname */

/**
 * Express app
 */

var pkg = require('../package.json'),
    morgan = require('morgan'),
    express = require('express'),
    finalhandler = require('finalhandler'),
    serveFavicon = require('serve-favicon'),
    request = require('request'),
    bodyParser = require('body-parser'),
    colors = require('colors/safe'),
    mode = process.env.CDAP_MODE || null,
    configParser = require('./config/configParser.js'),
    config = {},
    DIST_PATH = require('path').normalize(
      __dirname + '/../dist'
    );

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

// Body parser
app.use(bodyParser.json());

// serve the config file
app.get('/config.js', function (req, res) {

  configParser.extractConfig(mode, 'cdap', false /* isSecure*/)
    .then(function(config) {
      var data = JSON.stringify({
        // the following will be available in angular via the "MY_CONFIG" injectable

        authorization: req.headers.authorization,
        cdap: {
          routerServerUrl: config['router.server.address'],
          routerServerPort: config['router.server.port']
        }
      });

      res.header({
        'Content-Type': 'text/javascript',
        'Cache-Control': 'no-store, must-revalidate'
      });
      res.send('angular.module("'+pkg.name+'.config", [])' +
                '.constant("MY_CONFIG",'+data+');');
  });
});

app.post('/login', function (req, res) {
  if (!req.body.hasOwnProperty('username') || !req.body.hasOwnProperty('password')) {
    res.status(400).status('Please specify username and password');
  }
  var authAddress = 'http://localhost:10009/token';
  var options = {
    url: authAddress,
    auth: {
      user: req.body.username,
      password: req.body.password
    }
  };
  request(options, function (nerr, nres, nbody) {
    if (nerr || nres.statusCode !== 200) {
      res.status(400).send('Bad username and password.');
    } else {
      var nbody = JSON.parse(nbody);
      res.send({token: nbody.access_token});
    }
  });
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