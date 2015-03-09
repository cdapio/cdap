/*global require, module */


module.exports = {
  ping: function () {
    return require('./parser.js').extractConfig('cdap')
      .then(function (cdapConfig) {
        return (new AuthAddress()).doPing(cdapConfig);
      });
  }
};


var request = require('request'),
    promise = require('q');


var PING_INTERVAL = 1000,
    PING_MAX_RETRIES = 10000,
    PING_PATH = '/v3/ping';


function AuthAddress () {
  this.enabled = false;
  this.addresses = [];
};


/**
 * Ping the backend to figure out if auth is enabled.
 * @return {Promise} resolved with Security instance.
 */
AuthAddress.prototype.doPing = function (cdapConfig) {
  var self = this,
      deferred = promise.defer(),
      attempts = 0,
      url = cdapConfig['router.server.address'];

  if (cdapConfig['ssl.enabled'] === "true") {
    url = 'https://' + url + ':' + cdapConfig['router.ssl.server.port'];
  } else {
    url = 'http://' + url + ':' + cdapConfig['router.bind.port'];
  }
  url += PING_PATH;


  function pingAttempt () {
    attempts++;
    if (attempts > PING_MAX_RETRIES) {
      console.error('Exceeded max attempts ' + PING_MAX_RETRIES + ' calling secure endpoint.');
      deferred.reject();
    } else {
      console.log('Calling security endpoint: ', url, ' attempt ', attempts);
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
              self.addresses = JSON.parse(body).auth_uri || [];
            }
            console.info('Security is '+(self.enabled ? 'enabled': 'disabled')+'.');
            deferred.resolve(self);
          }
          else {
            setTimeout(pingAttempt, PING_INTERVAL);
          }
        }
      );
    }
  };

  if(process.env.CDAP_INSECURE) {
    console.info('[CDAP_INSECURE] Security is disabled.');
    deferred.resolve(self);
  }
  else {
    pingAttempt();
  }


  return deferred.promise;
};


/**
 * Picks an auth server address from options.
 * @return {String} Auth server address.
 */
AuthAddress.prototype.get = function () {
  if (!this.addresses.length) {
    return null;
  }
  return this.addresses[Math.floor(Math.random() * this.addresses.length)];
};

