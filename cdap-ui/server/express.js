/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

/* global require, module, process, __dirname */
var UrlValidator = require('./urlValidator.js');
var jwtDecode = require('jwt-decode');


module.exports = {
  getApp: function () {
    return require('q').all([
        // router check also fetches the auth server address if security is enabled
        require('./config/router-check.js').ping(),
        require('./config/parser.js').extractConfig('cdap'),
        require('./config/parser.js').extractUISettings()
      ])
      .spread(makeApp);
  }
};
var pkg = require('../package.json');
var path = require('path');
var express = require('express'),
    cookieParser = require('cookie-parser'),
    compression = require('compression'),
    finalhandler = require('finalhandler'),
    serveFavicon = require('serve-favicon'),
    request = require('request'),
    uuidV4 = require('uuid/v4'),
    log4js = require('log4js'),
    bodyParser = require('body-parser'),
    DLL_PATH = path.normalize(__dirname + '/../dll'),
    DIST_PATH = path.normalize(__dirname + '/../dist'),
    OLD_DIST_PATH = path.normalize(__dirname + '/../old_dist'),
    LOGIN_DIST_PATH= path.normalize(__dirname + '/../login_dist'),
    CDAP_DIST_PATH= path.normalize(__dirname + '/../cdap_dist'),
    MARKET_DIST_PATH= path.normalize(__dirname + '/../common_dist'),
    fs = require('fs'),
    objectQuery = require('lodash/get');

var log = log4js.getLogger('default');

const isModeDevelopment = () => process.env.NODE_ENV === 'development';
const isModeProduction = () => process.env.NODE_ENV === 'production';
const _headers = function(res) {
  res.set('Connection', 'close');
};

const getExpressStaticConfig = () => {
  if (isModeDevelopment()) {
    return {
      setHeaders: _headers
    };
  }
  return {
    index: false,
    maxAge: '1y',
    setHeaders: _headers
  };
};

function getFaviconPath(uiThemeConfig) {
  let faviconPath = DIST_PATH + '/assets/img/favicon.png';
  let themeFaviconPath = objectQuery(uiThemeConfig, ['content', 'favicon-path']);
  if (themeFaviconPath) {
    themeFaviconPath = CDAP_DIST_PATH + themeFaviconPath;
    try {
      if (require.resolve(themeFaviconPath)) {
        faviconPath = themeFaviconPath;
      }
    } catch (e) {
      log.warn(`Unable to find favicon at path ${themeFaviconPath}`);
    }
  }
  return faviconPath;
}

function extractUITheme(cdapConfig) {
  const uiThemePropertyName = 'ui.theme.file';
  const DEFAULT_CONFIG = {};

  if (!(uiThemePropertyName in cdapConfig)) {
    log.warn(`Unable to find ${uiThemePropertyName} property`);
    log.warn(`UI using default theme`);
    return DEFAULT_CONFIG;
  }

  let uiThemeConfig = DEFAULT_CONFIG;
  let uiThemePath = cdapConfig[uiThemePropertyName];
  if (uiThemePath[0] !== '/') {
    // if path doesn't start with /, then it's a relative path
    // have to add the ellipses to navigate back to $CDAP_HOME, before
    // going through the path
    uiThemePath = `../../${uiThemePath}`;
  }
  try {
    if (require.resolve(uiThemePath)) {
      uiThemeConfig = require(uiThemePath);
      log.info(`UI using theme located at ${cdapConfig[uiThemePropertyName]}`);
    }
  } catch (e) {
    // The error can either be file doesn't exist, or file contains invalid json
    log.error(e.toString());
    log.warn(`UI using default theme`);
  }
  return uiThemeConfig;
}

function makeApp (authAddress, cdapConfig, uiSettings) {
  var app = express();
  const urlValidator = new UrlValidator(cdapConfig);
  const uiThemeConfig = extractUITheme(cdapConfig);
  const faviconPath = getFaviconPath(uiThemeConfig);
  // middleware
  try { app.use(serveFavicon(faviconPath)); }
  catch (e) { log.error('Favicon missing! Please run `gulp build`'); }

  app.use(compression());
  app.use(bodyParser.json());
  app.use(bodyParser.urlencoded({ extended: false }));
  app.use(cookieParser());

  app.use(function (err, req, res, next) {
    log.error(err);
    res.status(500).send(err);
    next(err);
  });


  // serve the config file
  app.get('/config.js', function (req, res) {
    var data = JSON.stringify({
      // the following will be available in angular via the "MY_CONFIG" injectable

      authorization: req.headers.authorization,
      cdap: {
        routerServerUrl: cdapConfig['router.server.address'],
        routerServerPort: cdapConfig['router.server.port'],
        routerSSLServerPort: cdapConfig['router.ssl.server.port'],
        standaloneWebsiteSDKDownload: uiSettings['standalone.website.sdk.download'] === 'true' || false,
        uiDebugEnabled: uiSettings['ui.debug.enabled'] === 'true' || false
      },
      hydrator: {
        previewEnabled: cdapConfig['enable.preview'] === 'true'
      },
      marketUrl: cdapConfig['market.base.url'],
      sslEnabled: cdapConfig['ssl.external.enabled'] === 'true',
      securityEnabled: authAddress.enabled,
      isEnterprise: isModeProduction(),
      sandboxMode: process.env.NODE_ENV,
      authRefreshURL: cdapConfig['dashboard.auth.refresh.path'] || false,
      knoxEnabled: cdapConfig['knox.enabled'] === 'true',
      applicationPrefix: cdapConfig['application.prefix']
    });

    res.header({
      'Content-Type': 'text/javascript',
      'Cache-Control': 'no-store, must-revalidate',
      'Connection': 'close'
    });
    res.send('window.CDAP_CONFIG = '+data+';');
  });

   // serve the login config file
   app.get('/loginConfig', function (req, res) {
    var data = {
      marketUrl: cdapConfig['market.base.url'],
      sslEnabled: cdapConfig['ssl.external.enabled'] === 'true',
      knoxLoginUrl: cdapConfig['knox.login.url'],
      securityEnabled: authAddress.enabled,
      knoxEnabled: cdapConfig['knox.enabled'] === 'true',
      applicationPrefix: cdapConfig['application.prefix']
    };
    res.header({
      'Connection': 'close'
    });
    log.info('Data -> ', data);
    res.status(200).send(data);
  });

  app.get('/ui-config.js', function (req, res) {
    var path = __dirname + '/config/cdap-ui-config.json';

    var fileConfig = {};

    fileConfig = fs.readFileSync(path, 'utf8');
    res.header({
      'Content-Type': 'text/javascript',
      'Cache-Control': 'no-store, must-revalidate',
      'Connection': 'close'
    });
    res.send('window.CDAP_UI_CONFIG = ' + fileConfig+ ';');
  });

  app.get('/ui-theme.js', function (req, res) {
    res.header({
      'Content-Type': 'text/javascript',
      'Cache-Control': 'no-store, must-revalidate',
      'Connection': 'close'
    });
    res.send(`window.CDAP_UI_THEME = ${JSON.stringify(uiThemeConfig)};`);
  });

  app.post('/downloadQuery', function(req, res) {
    var url = req.body.backendUrl;
    res.header({
      'Connection': 'close'
    });
    if (!urlValidator.isValidURL(req.url)) {
      log.error('Bad Request');
      var err = {
        error: 400,
        message: 'Bad Request'
      };
      res.status(400).send(err);
      return;
    }
    log.info('Download Query Start: ', req.body.queryHandle);
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

  /**
   * This is used to stream content from Market directly to CDAP
   * ie. download app from market, and publish to CDAP
   *
   * Query parameters:
   *    source: Link to the content to forward
   *    sourceMethod: HTTP method to obtain content (default to GET)
   *    target: CDAP API
   *    targetMethod: HTTP method for the CDAP API (default to POST)
   **/
  app.get('/forwardMarketToCdap', function(req, res) {
    var sourceLink = req.query.source,
        targetLink = req.query.target,
        sourceMethod = req.query.sourceMethod || 'GET',
        targetMethod = req.query.targetMethod || 'POST';

    var forwardRequestObject = {
      url: targetLink,
      method: targetMethod,
      headers: req.headers
    };
    res.header({
      'Connection': 'close'
    });
    request({
      url: sourceLink,
      method: sourceMethod
    })
    .on('error', function (e) {
      log.error('Error', e);
    })
    .pipe(request(forwardRequestObject))
    .on('error', function (e) {
      log.error('Error', e);
    })
    .pipe(res);
  });

  app.get('/downloadLogs', function(req, res) {
    var url = decodeURIComponent(req.query.backendUrl);
    var method = (req.query.method || 'GET');
    res.header({
      'Connection': 'close'
    });
    if (!urlValidator.isValidURL(url)) {
      log.error('Bad Request');
      var err = {
        error: 400,
        message: 'Bad Request'
      };
      res.status(400).send(err);
      return;
    }
    log.info('Download Logs Start: ', url);
    var customHeaders;
    var requestObject = {
      method: method,
      url: url,
      rejectUnauthorized: false,
      requestCert: true,
      agent: false,
    };

    if (req.cookies['CDAP_Auth_Token']) {
      customHeaders = {
        authorization: 'Bearer ' + req.cookies['CDAP_Auth_Token']
      };
    }

    if (customHeaders) {
      requestObject.headers = customHeaders;
    }

    var type = req.query.type;
    var responseHeaders = {
      'Cache-Control': 'no-cache, no-store'
    };

    try {
      request(requestObject)
        .on('error', function (e) {
          log.error('Error request logs: ', e);
        })
        .on('response',
          function (response) {
            // This happens when use tries to access the link directly when
            // no autorization token present
            if (response.statusCode === 200) {
              if (type === 'download') {
                var filename = req.query.filename;
                responseHeaders['Content-Disposition'] = 'attachment; filename='+filename;
              } else {
                responseHeaders['Content-Type'] = 'text/plain';
              }

              res.set(responseHeaders);
            }
          }
        )
        .pipe(res)
        .on('error', function (e) {
          log.error('Error downloading logs: ', e);
        });
    } catch (e) {
      log.error('Downloading logs failed, ', e);
    }
  });

  /*
    For now both login and accessToken APIs do the same thing.
    This is only for semantic differntiation. In the future ideally
    these endpoints will vary based on success failure conditions.
    (A 404 vs warning for login vs token)

  */
  app.post('/login', authentication);

  app.post('/accessToken', authentication);

  app.get('/cdapToken', function (req, res) {
    // Invalid Knox Token Handler
    res.header({
      'Connection': 'close'
    });
    const onInvalidKnoxToken = function(errObj) {
      log.error('KNOX INVALID TOKEN', errObj);
          var err = {
            error: errObj,
            message: 'KNOX INVALID TOKEN',
          };
          res.status(err.statusCode?err.statusCode:402).send(err);
    };
    var knoxToken = req.cookies['hadoop-jwt'];
    if (!knoxToken) {
      onInvalidKnoxToken({error: 'Token not found'});
      return;
    }

    var userName = jwtDecode(knoxToken);
    if (!userName || !userName.sub) {
      onInvalidKnoxToken({error: 'Username not found'});
      return;
    }
    userName = userName.sub;
    var knoxUrl = [
      cdapConfig['ssl.external.enabled'] === 'true' ? 'https://' : 'http://',
      cdapConfig['router.server.address'],
      ':',
      cdapConfig['ssl.external.enabled'] === 'true' ? '10010' : '10009',
      '/knoxToken'
    ].join('');
    log.info('AUTH ->' + knoxUrl);
    var options = {
      url: knoxUrl,
      headers: {
        'knoxToken': knoxToken
      }
    };
    request(options, (error, response, body) => {
      if (error) {
        onInvalidKnoxToken(error);
      } else if (response) {
        if (response.statusCode === 200) {
          var respObj = {};
          if (body) {
            respObj = JSON.parse(body);
          }
          respObj['userName'] = userName;
          res.status(200).send(respObj);
        } else {
          onInvalidKnoxToken(response);
        }
      } else {
        onInvalidKnoxToken({ error: 'Error retrieving data' });
      }
    });
  });

  /*
    Handle POST requests made outside of the websockets from front-end.
    For now it handles file upload POST /namespaces/:namespace/apps API
  */
  app.post('/namespaces/:namespace/:path(*)', function (req, res) {
    var protocol,
        port;
    if (cdapConfig['ssl.external.enabled'] === 'true') {
      protocol = 'https://';
    } else {
      protocol = 'http://';
    }
    if (cdapConfig['ssl.external.enabled'] === 'true') {
      port = cdapConfig['router.ssl.server.port'];
    } else {
      port = cdapConfig['router.server.port'];
    }
    var headers = {};
    if (req.headers) {
      headers = req.headers;
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
      url: url,
      headers: {
        'Content-Type': headers['content-type']
      }
    };
    res.header({
      'Connection': 'close'
    });
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

  app.use('/old_assets', [
    express.static(OLD_DIST_PATH + '/assets', getExpressStaticConfig()),
    function(req, res) {
      finalhandler(req, res)(false); // 404
    }
  ]);
  // serve static assets
  app.use('/assets', [
    express.static(DIST_PATH + '/assets',{setHeaders: _headers}),
    function(req, res) {
      finalhandler(req, res)(false); // 404
    }
  ]);
  app.use('/cdap_assets', [
    express.static(CDAP_DIST_PATH + '/cdap_assets', getExpressStaticConfig()),
    function(req, res) {
      finalhandler(req, res)(false); // 404
    }
  ]);
  app.use('/dll_assets', [
    express.static(DLL_PATH, getExpressStaticConfig()),
    function(req, res) {
      finalhandler(req, res)(false);
    }
  ]);
  app.use('/login_assets', [
    express.static(LOGIN_DIST_PATH + '/login_assets', getExpressStaticConfig()),
    function(req, res) {
      finalhandler(req, res)(false); // 404
    }
  ]);
  app.use('/common_assets', [
    express.static(MARKET_DIST_PATH, {
      index: false,
      setHeaders: _headers
    }),
    function(req, res) {
      finalhandler(req, res)(false); // 404
    }
  ]);
  app.get('/robots.txt', [
    function (req, res) {
      res.header({
        'Connection': 'close'
      });
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
    res.header({
      'Connection': 'close'
    });
    request(opts,
      function (nerr, nres, nbody) {
        if (nerr || nres.statusCode !== 200) {
          var statusCode = (nres ? nres.statusCode : 500) || 500;
          res.status(statusCode).send(nbody);
        } else {
          res.send(nbody);
        }
      }
    );
  }


  app.get('/test/playground', [
    function (req, res) {
      res.header({
        'Connection': 'close'
      });
      res.sendFile(DIST_PATH + '/test.html');
    }
  ]);

  // CDAP-678, CDAP-8260 This is added for health check on node proxy.
  app.get('/status', function(req, res) {
    res.header({
      'Connection': 'close'
    });
    res.send(200, 'OK');
  });

  app.get('/login', [
    function(req, res) {
      res.header({
        'Connection': 'close'
      });
      if (!authAddress.get() || req.cookies.CDAP_Auth_Token) {
        res.redirect('/');
        return;
      }
      res.sendFile(LOGIN_DIST_PATH + '/login_assets/login.html');
    }
  ]);

  /*
    For now both login and accessToken APIs do the same thing.
    This is only for semantic differntiation. In the future ideally
    these endpoints will vary based on success failure conditions.
    (A 404 vs warning for login vs token)

  */
  app.post('/login', authentication);

  app.get('/backendstatus', [
    function (req, res) {
      var protocol,
          port;
      if (cdapConfig['ssl.external.enabled'] === 'true') {
        protocol = 'https://';
      } else {
        protocol = 'http://';
      }

      if (cdapConfig['ssl.external.enabled'] === 'true') {
        port = cdapConfig['router.ssl.server.port'];
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

      // FIXME: The reason for doing this is here: https://issues.cask.co/browse/CDAP-9059
      // TL;DR - The source of this issue is because of large browser urls
      // which gets added to headers while making /backendstatus http call.
      // That is the reason we are temporarily stripping out referer from the headers.
      var headers = req.headers;
      delete headers.referer;
      res.header({
        'Connection': 'close'
      });
      request({
        method: 'GET',
        url: link,
        rejectUnauthorized: false,
        requestCert: true,
        agent: false,
        headers: headers
      }, function (err, response) {
        if (err) {
          res.status(500).send(err);
        } else {
          res.status(response.statusCode).send('OK');
        }
      }).on('error', function (err) {
        try {
          res.status(500).send(err);
        } catch (e) {
          log.error('Failed sending exception to client', e);
        }
        log.error(err);
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
        res.header({
          'Connection': 'close'
        });
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
        } catch (e) {
          config.error = e.code;
          config.message = 'Error reading template - '+ apptype;
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
      res.header({
        'Connection': 'close'
      });
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
      } catch (e) {
        config.error = e.code;
        config.message = 'Error reading template - ' + appname + ' of type - ' + apptype;
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
      res.header({
        'Connection': 'close'
      });
      try {
        validators = JSON.parse(fs.readFileSync(filePath));
        res.send(validators);
      } catch (e) {
        config.error = e.code;
        config.message = 'Error reading validators.json';
        log.debug(config.message);
        res.status(404).send(config);
      }
    }
  ]);

  // any other path, serve index.html
  app.all(['/pipelines', '/pipelines*'], [
    function (req, res) {
      res.header({
        'Connection': 'close'
      });
      // BCookie is the browser cookie, that is generated and will live for a year.
      // This cookie is always generated to provide unique id for the browser that
      // is being used to interact with the CDAP backend.
      var date = new Date();
      date.setDate(date.getDate() + 365); // Expires after a year.
      if (!req.cookies.bcookie) {
        res.cookie('bcookie', uuidV4(), { expires: date });
      } else {
        res.cookie('bcookie', req.cookies.bcookie, { expires: date });
      }
     res.sendFile(DIST_PATH + '/hydrator.html');
    }
  ]);
  app.all(['/metadata', '/metadata*'], [
    function (req, res) {
      res.header({
        'Connection': 'close'
      });
      // BCookie is the browser cookie, that is generated and will live for a year.
      // This cookie is always generated to provide unique id for the browser that
      // is being used to interact with the CDAP backend.
      var date = new Date();
      date.setDate(date.getDate() + 365); // Expires after a year.
      if (!req.cookies.bcookie) {
        res.cookie('bcookie', uuidV4(), { expires: date });
      } else {
        res.cookie('bcookie', req.cookies.bcookie, { expires: date });
      }
     res.sendFile(DIST_PATH + '/tracker.html');
    }
  ]);

  app.all(['/logviewer', '/logviewer*'], [
    function (req, res) {
      res.header({
        'Connection': 'close'
      });
      // BCookie is the browser cookie, that is generated and will live for a year.
      // This cookie is always generated to provide unique id for the browser that
      // is being used to interact with the CDAP backend.
      var date = new Date();
      date.setDate(date.getDate() + 365); // Expires after a year.
      if (!req.cookies.bcookie) {
        res.cookie('bcookie', uuidV4(), { expires: date });
      } else {
        res.cookie('bcookie', req.cookies.bcookie, { expires: date });
      }
     res.sendFile(DIST_PATH + '/logviewer.html');
    }
  ]);

  app.all(['/', '/cdap', '/cdap*'], [
    function(req, res) {
      res.header({
        'Connection': 'close'
      });
      res.sendFile(CDAP_DIST_PATH + '/cdap_assets/cdap.html');
    }
  ]);

  app.get('/ui-config-old.js', function (req, res) {
    var path = __dirname + '/config/cdap-ui-config.json';

    var fileConfig = {};

    fileConfig = fs.readFileSync(path, 'utf8');
    res.header({
      'Content-Type': 'text/javascript',
      'Cache-Control': 'no-store, must-revalidate',
      'Connection': 'close'
    });
    res.send('angular.module("'+pkg.name+'.config")' +
              '.constant("UI_CONFIG",'+fileConfig+');');
  });
  app.get('/config-old.js', function (req, res) {

    var data = JSON.stringify({
      // the following will be available in angular via the "MY_CONFIG" injectable

      authorization: req.headers.authorization,
      cdap: {
        routerServerUrl: cdapConfig['router.server.address'],
        routerServerPort: cdapConfig['router.server.port'],
        routerSSLServerPort: cdapConfig['router.ssl.server.port']
      },
      hydrator: {
        previewEnabled: cdapConfig['enable.alpha.preview'] === 'true'
      },
      sslEnabled: cdapConfig['ssl.external.enabled'] === 'true',
      securityEnabled: authAddress.enabled,
      isEnterprise: isModeProduction()
    });

    res.header({
      'Content-Type': 'text/javascript',
      'Cache-Control': 'no-store, must-revalidate',
      'Connection': 'close'
    });
    res.send('angular.module("'+pkg.name+'.config", [])' +
              '.constant("MY_CONFIG",'+data+');');
  });

  app.all(['/oldcdap', '/oldcdap*'], [
    function (req, res) {
      res.header({
        'Connection': 'close'
      });
      res.sendFile(OLD_DIST_PATH + '/index.html');
    }
  ]);
  return app;

}
