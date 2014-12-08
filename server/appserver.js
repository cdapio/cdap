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
    http = require('http'),
    https = require('https'),
    mode = process.env.CDAP_MODE || null,
    DIST_PATH = require('path').normalize(
      __dirname + '/../dist'
    ),
    promise = require('q');

morgan.token('ms', function (req, res){
  if (!res._header || !req._startAt) { return ''; }
  var diff = process.hrtime(req._startAt);
  var ms = diff[0] * 1e3 + diff[1] * 1e-6;
  return Math.ceil(ms)+'ms';
});

var PING_FREQUENCY = 1000;
var MAX_RETRIES = 10;
var httpStaticLogger  = morgan(colors.green('http')+' :method :url :ms :status');
var httpIndexLogger  = morgan(colors.inverse('http')+' :method :url :ms :status');

/**
 * CDAP web app server.
 * @param {Object} config Object with key value pairs.
 * @returns {Promise} promise determining when server has started.
 */
var AppServer = function AppServer (config) {
  var deferred = promise.defer();
  var self = this;
  this.config = config;
  
  // Figure out a more graceful way of doing this.
  this.config['version'] = 2;
  
  this.setUpEnvironment()
  .then(function () {
    self.start();
    deferred.resolve(self.httpServer);
  })
  .catch(function () {
    console.info('Could not start server.');
    deferred.reject();
  });

  return deferred.promise;
};

AppServer.prototype.app = express();
AppServer.prototype.securityEnabled = false;
AppServer.prototype.authServerAddresses = [];

/**
 * Configures SSL, sets up environment and determines if security is active.
 * @returns {Promise} Executed upon figuring out if security is enabled.
 */
AppServer.prototype.setUpEnvironment = function setUpEnvironment () {
  var self = this;
  var deferred = promise.defer();
  if (this.config['ssl.enabled'] === 'true') {
    this.lib = https;
    if (this.config["dashboard.ssl.disable.cert.check"] === "true") {
      /*
        For self signed certs
        The github issue in relation to this is : https://github.com/mikeal/request/issues/418
      */
      process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
    }
  } else {
    this.lib = http;
  }

  var path = '/v' + this.config.version + '/ping',
      url;

  if (this.config['ssl.enabled'] === "true") {
    url = ('https://' + this.config['router.server.address'] + ':' 
      + this.config['router.ssl.server.port'] + path);
  } else {
    url = ('http://' + this.config['router.server.address'] + ':' 
      + this.config['router.bind.port'] + path);
  }

  var attempts = 0;
  var interval = setInterval(function () {
    attempts++;
    if (attempts > MAX_RETRIES) {
      clearInterval(interval);
      console.info('Exceeded max attempts calling secure endpoint.');
      deferred.reject();
    } else {
      console.info('Calling security endpoint: ', url, ' attempt ', attempts);
      var options = {
        method: 'GET',
        url: url,
        rejectUnauthorized: false,
        requestCert: true,
        agent: false
      };
      request(options, function (err, response, body) {
        if (!err && response) {
          clearInterval(interval);
          if (body) {
            if (response.statusCode === 401) {
              self.securityEnabled = true;
              self.authServerAddresses = JSON.parse(body).auth_uri || [];
            } else {
              self.securityEnabled = false;
            }
          } else {
            self.securityEnabled = false;
          }
          console.info('Security configuration found. Security is enabled:', self.securityEnabled);
          deferred.resolve();
        }
      });
    }
  }, PING_FREQUENCY);

  return deferred.promise;
};

/**
 * Picks an auth server address from options.
 * @return {String} Auth server address.
 */
AppServer.prototype.getAuthServerAddress = function () {
  if (!this.authServerAddresses.length) {
    return null;
  }
  return this.authServerAddresses[Math.floor(Math.random() * this.authServerAddresses.length)];
};

/**
 * Configures express server and attaches it to app server.
 */
AppServer.prototype.start = function start () {
  var self = this;
  console.log(colors.underline(pkg.name) + ' v' + pkg.version + ' starting up...');

  try { this.app.use(serveFavicon(DIST_PATH + '/assets/img/favicon.png')); }
  catch(e) { console.error('Favicon missing! Did you run `gulp build`?'); }
  // Body parser
  this.app.use(bodyParser.json());


  // serve the config file
  this.app.get('/config.js', function (req, res) {

    var data = JSON.stringify({
      // the following will be available in angular via the "MY_CONFIG" injectable

      authorization: req.headers.authorization,
      cdap: {
        routerServerUrl: self.config['router.server.address'],
        routerServerPort: self.config['router.server.port']
      },
      securityEnabled: self.securityEnabled
    });

    res.header({
      'Content-Type': 'text/javascript',
      'Cache-Control': 'no-store, must-revalidate'
    });
    res.send('angular.module("'+pkg.name+'.config", [])' +
              '.constant("MY_CONFIG",'+data+');');
  });


  this.app.post('/login', function (req, res) {
    if (!req.body.hasOwnProperty('username') || !req.body.hasOwnProperty('password')) {
      res.send(400, "Please specify username/password");
    }
    var authAddress = self.getAuthServerAddress();
    var options = {
      url: authAddress,
      auth: {
        user: req.body.username,
        password: req.body.password
      }
    };
    request(options, function (nerr, nres, nbody) {
      if (nerr || nres.statusCode !== 200) {
        res.status(nres.statusCode).send(nbody);
      } else {
        var nbody = JSON.parse(nbody);
        res.send({token: nbody.access_token});
      }
    });
  });

  // serve static assets
  this.app.use('/assets', [
    httpStaticLogger,
    express.static(DIST_PATH + '/assets', {
      index: false
    }),
    function(req, res) {
      finalhandler(req, res)(false); // 404
    }
  ]);

  this.app.get('/robots.txt', [
    httpStaticLogger,
    function (req, res) {
      res.type('text/plain');
      res.send('User-agent: *\nDisallow: /');
    }
  ]);


  // any other path, serve index.html
  this.app.all('*', [
    httpIndexLogger,
    function (req, res) {
      res.sendFile(DIST_PATH + '/index.html');
    }
  ]);


  this.httpServer = this.lib.createServer(this.app);
};

module.exports = AppServer;