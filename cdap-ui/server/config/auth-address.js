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
    log4js = require('log4js'),
    promise = require('q');

var log = log4js.getLogger('default');


var PING_INTERVAL = 1000,
    PING_MAX_RETRIES = 1000,
    PING_PATH = '/ping';


function AuthAddress () {
  this.enabled = false;
  this.addresses = [];
}


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

    log.debug('Checking backend security endpoint ' + url + ' attempt ' + attempts);
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
          log.info('CDAP security is '+(self.enabled ? 'enabled': 'disabled')+'.');
          deferred.resolve(self);
        }
        else {
          setTimeout(pingAttempt, attempts<PING_MAX_RETRIES ? PING_INTERVAL : PING_INTERVAL*60);
          deferred.resolve(self);
        }
      }
    );
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
