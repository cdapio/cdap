/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/*global require, module, process, __dirname */

module.exports = {
  getApp: function () {
    return require('q').all([
        // router check also fetches the auth server address if security is enabled
        require('./config/router-check.js').ping(),
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
    ),
    fs = require('fs');

var log = log4js.getLogger('default');

function makeApp (authAddress, cdapConfig) {

  var app = express();

  // middleware
  try { app.use(serveFavicon(DIST_PATH + '/assets/img/favicon.png')); }
  catch(e) { log.error('Favicon missing! Please run `gulp build`'); }

  // Check environment variable CDAP_UI_COMPRESSION_ENABLED
  // Default to true, unless this variable is set to something else
  if (typeof process.env.CDAP_UI_COMPRESSION_ENABLED === 'undefined' ||
      process.env.CDAP_UI_COMPRESSION_ENABLED === true ||
      process.env.CDAP_UI_COMPRESSION_ENABLED === 'true') {
    app.use(compression());
  }

  app.use(bodyParser.json());
  app.use(bodyParser.urlencoded({ extended: false }));
  app.use(cookieParser());

  // serve the config file
  app.get('/config.js', function (req, res) {

    var data = JSON.stringify({
      // the following will be available in angular via the "MY_CONFIG" injectable

      authorization: req.headers.authorization,
      cdap: {
        routerServerUrl: cdapConfig['router.server.address'],
        routerServerPort: cdapConfig['router.server.port'],
        routerSSLServerPort: cdapConfig['router.ssl.bind.port']
      },
      sslEnabled: cdapConfig['ssl.enabled'] === 'true',
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

  app.get('/ui-config.js', function (req, res) {
    var path = __dirname + '/config/cdap-ui-config.json';

    var fileConfig = {};

    fileConfig = fs.readFileSync(path, 'utf8');
    res.header({
      'Content-Type': 'text/javascript',
      'Cache-Control': 'no-store, must-revalidate'
    });
    res.send('angular.module("'+pkg.name+'.config")' +
              '.constant("UI_CONFIG",'+fileConfig+');');
  });

  app.post('/downloadQuery', function(req, res) {
    var url = req.body.backendUrl;

    log.info('Download Start: ', req.body.queryHandle);

    request({
      method: 'POST',
      url: url,
      rejectUnauthorized: false,
      requestCert: true,
      agent: false,
      headers: req.headers
    })
    .on('error', function (e) {
      log.error('Error request: ', e);
    })
    .pipe(res)
    .on('error', function (e) {
      log.error('Error downloading query: ', e);
    });

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
    var protocol,
        port;
    if (cdapConfig['ssl.enabled'] === 'true') {
      protocol = 'https://';
    } else {
      protocol = 'http://';
    }
    if (cdapConfig['ssl.enabled'] === 'true') {
      port = cdapConfig['router.ssl.bind.port'];
    } else {
      port = cdapConfig['router.server.port'];
    }

    var url = [
      protocol,
      cdapConfig['router.server.address'],
      ':',
      port,
      '/v3/namespaces/',
      req.param('namespace'),
      '/',
      req.param('path')
    ].join('');

    var opts = {
      method: 'POST',
      url: url
    };

    req
      .on('error', function (e) {
        log.error(e);
      })
      .pipe(request.post(opts))
        .on('error', function (e) {
          log.error(e);
        })
      .pipe(res)
        .on('error', function (e) {
          log.error(e);
        });
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
      var protocol,
          port;
      if (cdapConfig['ssl.enabled'] === 'true') {
        protocol = 'https://';
      } else {
        protocol = 'http://';
      }

      if (cdapConfig['ssl.enabled'] === 'true') {
        port = cdapConfig['router.ssl.bind.port'];
      } else {
        port = cdapConfig['router.server.port'];
      }

      var link = [
        protocol,
        cdapConfig['router.server.address'],
        ':',
        port,
        '/v3/namespaces'
      ].join('');

      request({
        method: 'GET',
        url: link,
        rejectUnauthorized: false,
        requestCert: true,
        agent: false,
        headers: req.headers
      }, function (err, response) {
        if (err) {
          if (err.code === 'ECONNREFUSED') {
            res.status(404).send(err);
            return;
          }
          res.status(500).send(err);
        } else {
          res.status(response.statusCode).send('OK');
        }
      });
    }
  ]);

  app.get('/predefinedapps/:apptype', [
      function (req, res) {
        var apptype = req.params.apptype;
        var config = {};
        var fileConfig = {};
        var filesToMetadataMap = [];
        var filePath = __dirname + '/../templates/apps/predefined/config.json';
        try {
          fileConfig = JSON.parse(fs.readFileSync(filePath, 'utf8'));
          filesToMetadataMap = fileConfig[apptype] || [];
          if (filesToMetadataMap.length === 0) {
            throw {code: 404};
          }
          filesToMetadataMap = filesToMetadataMap.map(function(metadata) {
            return {
              name: metadata.name,
              description: metadata.description
            };
          });
          res.send(filesToMetadataMap);
        } catch(e) {
          config.error = e.code;
          config.message = 'Error reading template - '+ apptype ;
          log.debug(config.message);
          res.status(404).send(config);
        }
      }
  ]);

  app.get('/predefinedapps/:apptype/:appname', [
    function (req, res) {
      var apptype = req.params.apptype;
      var appname = req.params.appname;
      var filesToMetadataMap = [];
      var appConfig = {};

      var dirPath = __dirname + '/../templates/apps/predefined/';
      var filePath = dirPath + 'config.json';
      var config = {};
      var fileConfig = {};
      try {
        fileConfig = JSON.parse(fs.readFileSync(filePath, 'utf8'));
        filesToMetadataMap = fileConfig[apptype] || [];
        filesToMetadataMap = filesToMetadataMap.filter(function(metadata) {
          if (metadata.name === appname) {
            return metadata.file;
          }
        });
        if (filesToMetadataMap.length === 0) {
          throw {code: 404};
        }
        appConfig = JSON.parse(
          fs.readFileSync(dirPath + '/' + filesToMetadataMap[0].file)
        );
        res.send(appConfig);
      } catch(e) {
        config.error = e.code;
        config.message = 'Error reading template - ' + appname + ' of type - ' + apptype ;
        log.debug(config.message);
        res.status(404).send(config);
      }
    }
  ]);

  app.get('/validators', [
    function (req, res) {
      var filePath = __dirname + '/../templates/validators/validators.json';
      var config = {};
      var validators = {};

      try {
        validators = JSON.parse(fs.readFileSync(filePath));
        res.send(validators);
      } catch(e) {
        config.error = e.code;
        config.message = 'Error reading validators.json';
        log.debug(config.message);
        res.status(404).send(config);
      }
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
