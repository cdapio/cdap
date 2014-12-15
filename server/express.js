/*global require, module, process, __dirname */

module.exports = {
  getApp: function () {
    return require('q').all([
        require('./config/auth-address.js').ping(),
        require('./config/parser.js').extractConfig('cdap')
      ])
      .spread(makeApp);
  }
};

var pkg = require('../package.json'),
    morgan = require('morgan'),
    express = require('express'),
    compression = require('compression'),
    finalhandler = require('finalhandler'),
    serveFavicon = require('serve-favicon'),
    request = require('request'),
    bodyParser = require('body-parser'),
    colors = require('colors/safe'),
    DIST_PATH = require('path').normalize(
      __dirname + '/../dist'
    );

morgan.token('ms', function (req, res){
  if (!res._header || !req._startAt) { return ''; }
  var diff = process.hrtime(req._startAt);
  var ms = diff[0] * 1e3 + diff[1] * 1e-6;
  return Math.ceil(ms)+'ms';
});

var httpStaticLogger = morgan(colors.green('http')+' :method :url :ms :status');
var httpIndexLogger = morgan(colors.inverse('http')+' :method :url :ms :status');

function makeApp (authAddress, cdapConfig) {

  var app = express();
  console.log(colors.underline(pkg.name) + ' v' + pkg.version + ' starting up...');

  // middleware
  try { app.use(serveFavicon(DIST_PATH + '/assets/img/favicon.png')); }
  catch(e) { console.error('Favicon missing! Did you run `gulp build`?'); }
  app.use(compression());
  app.use(bodyParser.json());

  // serve the config file
  app.get('/config.js', function (req, res) {

    var data = JSON.stringify({
      // the following will be available in angular via the "MY_CONFIG" injectable

      authorization: req.headers.authorization,
      cdap: {
        routerServerUrl: cdapConfig['router.server.address'],
        routerServerPort: cdapConfig['router.server.port']
      },
      securityEnabled: authAddress.enabled
    });

    res.header({
      'Content-Type': 'text/javascript',
      'Cache-Control': 'no-store, must-revalidate'
    });
    res.send('angular.module("'+pkg.name+'.config", [])' +
              '.constant("MY_CONFIG",'+data+');');
  });


  // forward login requests
  app.post('/login', function (req, res) {
    if (!req.body.username || !req.body.password) {
      res.status(400).send('Please specify username/password');
    }
    request({
        url: authAddress.get(),
        auth: {
          user: req.body.username,
          password: req.body.password
        }
      },
      function (nerr, nres, nbody) {
        if (nerr || nres.statusCode !== 200) {
          res.status(nres.statusCode).send(nbody);
        } else {
          res.send(nbody);
        }
      }
    );
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

  return app;

}


