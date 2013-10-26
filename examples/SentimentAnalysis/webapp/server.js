/**
 * Copyright (c) 2013 Continuuity, Inc.
 * Server for external site.
 */
var express = require('express'),
  fs = require('fs'),
  log4js = require('log4js'),
  cons = require('consolidate'),
  swig = require('swig'),
  request = require('request');

/*
 * Configure logger for debugging.
 */
log4js.configure({
  appenders: [
    {type: 'console'}
  ]
});

/**
 * Set up site namespace.
 */
var site = site || {};

/**
 * Attach logger to site.
 */
site.logger = log4js.getLogger('Developer');
site.logger.setLevel(site.LOG_LEVEL);

/**
 * Specify logger level.
 */
site.LOG_LEVEL = 'INFO';

/*
 * Specify on which port server should run.
 */
site.PORT = 8000;

/**
 * App framework.
 */
site.app = express();

/**
 * Configure static files server.
 */
site.app.use(express.cookieParser());
site.app.use(express.bodyParser());
site.app.use('/static', express.static(__dirname + '/'));

/**
 * Parses through each request and checks if request is from mobile and sets cookie.
 */

/**
 * Configure templating engine.
 * @param {string} templateDir location of the template directory.
 */
site.configureTemplating = function(templateDir) {
  // Initialize swig.
  swig.Swig({
    root: templateDir,
    allowErrors: true
  });
  site.app.set('views', templateDir);
};

site.app.engine('.html', cons.swig);
site.app.set('view engine', 'html');
site.app.set('view options', { layout: false });

site.configureTemplating(site.TEMPLATE_DIR);

/**
 * Error handling middleware.
 */
site.app.use(express.methodOverride());
site.app.use(site.app.router);

site.app.get('/', function(req, res) {

  res.sendfile('index.html');

});

site.app.get('/getSentimentAggregates', function(req, res) {

  var opts = {url: 'http://127.0.0.1:10000/v2/apps/sentiment/procedures/sentiment-query/methods/aggregates'};

  request.get(opts, function (error, response, body) {
    res.send(body);
  });
});

site.app.get('/getSentimentsPositive', function(req, res) {

  var opts = {url: 'http://127.0.0.1:10000/v2/apps/sentiment/procedures/sentiment-query/methods/sentiments?sentiment=positive'};
  request.get(opts, function (error, response, body) {
    res.send(body);
  });

});

site.app.get('/getSentimentsNeutral', function(req, res) {

  var opts = {url: 'http://127.0.0.1:10000/v2/apps/sentiment/procedures/sentiment-query/methods/sentiments?sentiment=neutral'};
  request.get(opts, function (error, response, body) {
    res.send(body);
  });
});

site.app.get('/getSentimentsNegative', function(req, res) {
  var opts = {url: 'http://127.0.0.1:10000/v2/apps/sentiment/procedures/sentiment-query/methods/sentiments?sentiment=negative'};
  request.get(opts, function (error, response, body) {
    res.send(body);
  });

});

site.app.post('/injectToStream', function(req, res) {
  var opts = {url: 'http://127.0.0.1:10000/v2/streams/sentence'};
  if (req.body) {
    opts.body = req.body.data;
  }
  request.post(opts, function (error, response, body) {
    res.send(body);
  });
});

/**
 * Error page for testing 500 status code.
 */
site.app.get('/error', function(req, res) {
  throw new Error();
});


/**
 * Status for checking if site is up.
 */
site.app.get('/status', function(req, res) {
  res.send('ok');
});

/*
 * Current version of developer suite.
 */
site.app.get('/version', function(req, res) {

  var version = fs.readFileSync(__dirname + '/version', 'utf8');
  res.send(version);

});

/**
 * Send last route for a page not found page.
 * !!!Keep this as the last route on the page.
 */
site.app.use(function (req, res, next) {
  res.status(404);

  // respond with html page
  if (req.accepts('html')) {
    res.sendfile('404.html', { url: req.url });
    return;
  }
  next();
});

/*
 * Start express server on specified port and show message if successful.
 */
site.app.listen(site.PORT);
site.logger.info('Server started on port ', site.PORT);

/**
 * Export module.
 */
module.exports = site;
