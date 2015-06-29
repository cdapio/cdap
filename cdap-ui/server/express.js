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
    express = require('express'),
    cookieParser = require('cookie-parser'),
    compression = require('compression'),
    finalhandler = require('finalhandler'),
    serveFavicon = require('serve-favicon'),
    request = require('request'),
    uuid = require('node-uuid'),
    log4js = require('log4js'),
    bodyParser = require('body-parser'),
    DIST_PATH = require('path').normalize(
      __dirname + '/../dist'
    );

var log = log4js.getLogger('default');

function makeApp (authAddress, cdapConfig) {

  var app = express();

  // middleware
  try { app.use(serveFavicon(DIST_PATH + '/assets/img/favicon.png')); }
  catch(e) { log.error('Favicon missing! Please run `gulp build`'); }
  app.use(compression());
  app.use(bodyParser.json());
  app.use(cookieParser());

  // serve the config file
  app.get('/config.js', function (req, res) {

    var data = JSON.stringify({
      // the following will be available in angular via the "MY_CONFIG" injectable

      authorization: req.headers.authorization,
      cdap: {
        routerServerUrl: cdapConfig['router.server.address'],
        routerServerPort: cdapConfig['router.server.port']
      },
      securityEnabled: authAddress.enabled,
      isEnterprise: process.env.NODE_ENV === 'production'
    });

    res.header({
      'Content-Type': 'text/javascript',
      'Cache-Control': 'no-store, must-revalidate'
    });
    res.send('angular.module("'+pkg.name+'.config", [])' +
              '.constant("MY_CONFIG",'+data+');');
  });


  /*
    For now both login and accessToken APIs do the same thing.
    This is only for semantic differntiation. In the future ideally
    these endpoints will vary based on success failure conditions.
    (A 404 vs warning for login vs token)

  */
  app.post('/login', authentication);

  app.post('/accessToken', authentication);

  /*
    Handle POST requests made outside of the websockets from front-end.
    For now it handles file upload POST /namespaces/:namespace/apps API
  */
  app.post('/namespaces/:namespace/:path(*)', function (req, res) {
    var url = 'http://' + cdapConfig['router.server.address'] +
              ':' +
              cdapConfig['router.server.port'] +
              '/v3/namespaces/' +
              req.param('namespace') +
              '/' + req.param('path');

    var opts = {
      method: 'POST',
      url: url
    };

    req.pipe(request.post(opts)).pipe(res);
  });

  // serve static assets
  app.use('/assets', [
    express.static(DIST_PATH + '/assets', {
      index: false
    }),
    function(req, res) {
      finalhandler(req, res)(false); // 404
    }
  ]);

  app.get('/robots.txt', [
    function (req, res) {
      res.type('text/plain');
      res.send('User-agent: *\nDisallow: /');
    }
  ]);

  function authentication(req, res) {
    var opts = {
      auth: {
        user: req.body.username,
        password: req.body.password
      },
      url: authAddress.get()
    };

    request(opts,
      function (nerr, nres, nbody) {

        if (nerr || nres.statusCode !== 200) {
          res.status(nres.statusCode).send(nbody);
        } else {
          res.send(nbody);
        }
      }
    );
  }

  app.get('/test/playground', [
    function (req, res) {
      res.sendFile(DIST_PATH + '/test.html');
    }
  ]);

  app.get('/backendstatus', [
    function (req, res) {

      var link = 'http://' + cdapConfig['router.server.address'] +
              ':' +
              cdapConfig['router.server.port'] +
              '/v3/namespaces';

      req.pipe(
        request({
          method: 'GET',
          url: link,
          rejectUnauthorized: false,
          requestCert: true,
          agent: false,
          headers: req.headers
        })
      ).pipe(res);
    }
  ]);

  // any other path, serve index.html
  app.all('*', [
    function (req, res) {
      // BCookie is the browser cookie, that is generated and will live for a year.
      // This cookie is always generated to provide unique id for the browser that
      // is being used to interact with the CDAP backend.
      var date = new Date();
      date.setDate(date.getDate() + 365); // Expires after a year.
      if(! req.cookies.bcookie) {
        res.cookie('bcookie', uuid.v4(), { expires: date});
      } else {
        res.cookie('bcookie', req.cookies.bcookie, { expires: date});
      }
     res.sendFile(DIST_PATH + '/index.html');
    }
  ]);


  return app;

}
