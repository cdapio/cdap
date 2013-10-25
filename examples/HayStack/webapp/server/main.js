
var express = require('express'),
  http = require('http'),
  querystring = require('querystring'),
  crypto = require('crypto'),
  http = require('http'),
  log4js = require('log4js'),
  facebook = require('facebook-session-express'),
  config = require('./haystack.conf.js');

http.globalAgent.maxSockets = 50;

/**
 * Configure Express
 */
var App = express();
App.use(express.static(__dirname + '/../client/'));
App.use(express.bodyParser());
App.use(express.cookieParser());

/**
 * Configure the logger.
 */
var LOG_LEVEL = 'TRACE';
log4js.configure({
  appenders: [
    {
      type : 'console'
    },
  ]
});
var logger = log4js.getLogger('Streamy Lite');
logger.setLevel(LOG_LEVEL);

/**
 * Configure Reactor Procedures
 */
var Procedures = require('./procedures');
Procedures.configure(config);

App.use(function (req, res, next) {

  logger.trace(req.url);
  next();

});

/*
 * Read Stories for a given Stream.
 */
App.get('/logs', function (req, res) {

  Procedures.Logs.getLogs(null, res);

});

/*
 * Run search.
 */
App.get('/search', function (req, res) {

console.log(req.query);
  Procedures.Search.search(req.query, res);

});

/*
 * Get trends.
 */
App.get('/trend', function (req, res) {

  Procedures.Trend.trend(req.query, res);

});

/*
 * Get alerts.
 */
App.get('/alerts', function (req, res) {

  Procedures.Alerts.alerts(req.query, res);

});

function sendToStream (name, payload, callback) {

  var content = JSON.stringify(payload);

  var options = {
      host: config['reactor.hostname'],
      port: config['reactor.gateway.port'],
      path: '/v2/streams/' + name,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': content.length
      }
    };

  var request = http.request(options, function(response) {
    var data = '';
    response.on('data', function (chunk) {
      data += chunk;
    });

    response.on('end', function () {

      try {
        if (data === '') {
          callback({
            error: 'Please make sure the Reactor application is running.'
          });
        } else {
          callback(JSON.parse(data));
        }
      } catch (e) {
        callback({
          error: e.message + ': "' + data + '"'
        });
      }

    });
  });

  request.on('error', function (e) {

    callback({
      error: 'Could not connect to Reactor. Please check your configuration.'
    });

  });

  logger.info('User Action', payload);

  request.write(content);
  request.end();

}

// This is an example.
App.post('/send/to/stream', function (req, res) {

  var payload = {
    id: 'abc'
  };

  sendToStream('my-stream', payload, function () {

    res.send(200);

  });

});

/*
 * Catchall
 */

App.get('*', function (req, res) {

  res.send(404);

});

/*
 * Start
 */

http.createServer(App).listen(8080);
console.log('Listening on port 8080.');
