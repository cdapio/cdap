/*global require, module */

module.exports = {
  promise: function () {
    return configParser.promise().then(function (config) {
      return (new Security(config)).doPing();
    });
  }
};


var configParser = require('./parser.js'),
    request = require('request'),
    promise = require('q');

var PING_INTERVAL = 1000,
    PING_MAX_RETRIES = 30,
    PING_PATH = '/v2/ping';


function Security (config) {
  this.cdapConfig = config || {};
  this.enabled = false;
  this.authServerAddresses = [];
};


/**
 * Ping the backend to figure out if auth is enabled.
 * @return {Promise} resolved with Security instance.
 */
Security.prototype.doPing = function () {
  var self = this,
      deferred = promise.defer(),
      attempts = 0,
      url = this.cdapConfig['router.server.address'];

  if (this.cdapConfig['ssl.enabled'] === "true") {
    url = 'https://' + url + ':' + this.cdapConfig['router.ssl.server.port'];
  } else {
    url = 'http://' + url + ':' + this.cdapConfig['router.bind.port'];
  }
  url += PING_PATH;


  function pingAttempt () {
    attempts++;
    if (attempts > PING_MAX_RETRIES) {
      console.error('Exceeded max attempts calling secure endpoint.');
      deferred.reject();
    } else {
      // console.log('Calling security endpoint: ', url, ' attempt ', attempts);
      request({
          method: 'GET',
          url: url,
          rejectUnauthorized: false,
          requestCert: true,
          agent: false
        },
        function (err, response, body) {
          if (!err && response) {
            if (response.statusCode === 401) {
              self.enabled = true;
              self.authServerAddresses = JSON.parse(body).auth_uri || [];
            }
            console.info('Security is '+(self.enabled ? 'enabled': 'disabled'));
            deferred.resolve(self);
          }
          else {
            setTimeout(pingAttempt, PING_INTERVAL);
          }
        }
      );
    }
  };

  pingAttempt();

  return deferred.promise;
};


/**
 * Picks an auth server address from options.
 * @return {String} Auth server address.
 */
Security.prototype.getAuthServerAddress = function () {
  if (!this.authServerAddresses.length) {
    return null;
  }
  return this.authServerAddresses[Math.floor(Math.random() * this.authServerAddresses.length)];
};

