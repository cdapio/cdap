// @ts-nocheck
/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

import request from 'request';
import fs from 'fs';
import log4js from 'log4js';
import * as urlHelper from 'server/url-helper';
import { extractConfig } from 'server/config/parser';
import * as sessionToken from 'server/token';

const log = log4js.getLogger('default');
/**
 * Aggregator
 * receives resourceObj, aggregate them,
 * and send poll responses back through socket
 *
 * @param {Object} SockJS connection
 */
function Aggregator(conn) {
  // make 'new' optional
  if (!(this instanceof Aggregator)) {
    return new Aggregator(conn);
  }
  this.cdapConfig = null;
  this.connection = conn;

  this.populateCdapConfig().then(this.initializeEventListeners.bind(this));
  this.isSessionValid = false;
}

Aggregator.prototype.initializeEventListeners = function() {
  /**
   * Handler for data from client via websocket connection
   * Checks for the session token in the first message.
   * If valid sets the isSessionValid flag to true and proceeds to skip
   * validations for subsequent messages.
   */
  this.connection.on('data', (message) => {
    if (this.isSessionValid) {
      return onSocketData.call(this, message);
    }
    if (!this.validateSession(message)) {
      return;
    }
    this.isSessionValid = true;
    onSocketData.call(this, message);
  });
  this.connection.on('close', onSocketClose.bind(this));
};

Aggregator.prototype.validateSession = function(message) {
  let messageJSON;
  try {
    /**
     * Closes the connection if the session is not valid.
     */
    messageJSON = JSON.parse(message);
    let authToken = '';
    if (
      messageJSON.resource &&
      messageJSON.resource.headers &&
      messageJSON.resource.headers.Authorization
    ) {
      authToken = messageJSON.resource.headers.Authorization;
    }
    if (!sessionToken.validateToken(messageJSON.sessionToken, this.cdapConfig, log, authToken)) {
      log.error('Found invalid session token. Closing websocket connection');
      this.connection.end();
      onSocketClose.call(this);
      return false;
    }
  } catch (e) {
    log.error('Unable to parse message : ' + e);
    return false;
  }
  return true;
};

Aggregator.prototype.populateCdapConfig = async function() {
  let cdapConfig, securityConfig;
  try {
    cdapConfig = await extractConfig('cdap');
    securityConfig = await extractConfig('security');
    this.cdapConfig = { ...cdapConfig, ...securityConfig };
  } catch (e) {
    log.error(
      "[ERROR]: Unable to parse CDAP config. CDAP UI Proxy won't be able to serve any request to the client: " +
        e
    );
  }
};

/**
 * Pushes the ETL Application configuration for templates and plugins to the
 * FE. These configurations are UI specific and hences need to be supported
 * here.
 */
Aggregator.prototype.pushConfiguration = function(resource) {
  var templateid = resource.templateid;
  var pluginid = resource.pluginid;
  var configString;
  var config = {};
  var statusCode = 404;
  var filePaths = [];
  var isConfigSemanticsValid;
  // Some times there might a plugin that is common across multiple templates
  // in which case, this is stored within the common directory. So, if the
  // template specific plugin check fails, then attempt to get it from common.
  filePaths.push(
    __dirname + '/../templates/' + templateid + '/' + pluginid + '.json',
    __dirname + '/../templates/common/' + pluginid + '.json'
  );
  var i,
    paths = filePaths.length;
  var fileFound = true;

  // Check if the configuration is present within the plugin for a template
  for (i = 0; i < paths; i++) {
    try {
      configString = fs.readFileSync(filePaths[i], 'utf8');
      statusCode = 200;
      fileFound = true;
      break;
    } catch (e) {
      if (e.code === 'ENOENT') {
        fileFound = false;
      }
    }
  }
  if (!fileFound) {
    statusCode = 404;
    config = 'NO_JSON_FOUND';
  } else {
    try {
      config = JSON.parse(configString);
      statusCode = 200;
    } catch (e) {
      statusCode = 500;
      config = 'CONFIG_SYNTAX_JSON_ERROR';
    }
  }

  if (statusCode === 200 && !(config.metadata && config.metadata['spec-version'])) {
    isConfigSemanticsValid = validateSemanticsOfConfigJSON(config);
    if (!isConfigSemanticsValid) {
      statusCode = 500;
      config = 'CONFIG_SEMANTICS_JSON_ERROR';
    }
  }

  this.connection.write(
    JSON.stringify({
      resource: resource,
      statusCode: statusCode,
      response: config,
    })
  );
};

function validateSemanticsOfConfigJSON(config) {
  var groups = config.groups.position;
  var groupsMap = config.groups;
  var i, j;
  var isValid = true;
  var fields, fieldsMap;

  for (i = 0; i < groups.length; i++) {
    if (!groupsMap[groups[i]] || !isValid) {
      isValid = false;
      break;
    }

    fields = groupsMap[groups[i]].position;
    fieldsMap = groupsMap[groups[i]].fields;

    if (!fields || !fieldsMap) {
      isValid = false;
    } else {
      for (j = 0; j < fields.length; j++) {
        if (!fieldsMap[fields[j]]) {
          isValid = false;
          break;
        }
      }
    }
  }

  return isValid;
}

/**
 * Helps avoid sending certain properties to the browser (meta attributes used only in the node server)
 */
function stripResource(key, value) {
  // note that 'stop' is not the stop timestamp, but rather a stop flag/signal (unlike the startTs)
  if (key === 'timerId' || key === 'startTs' || key === 'stop') {
    return undefined;
  }
  return value;
}

/**
 * @private emitResponse
 *
 * sends data back to the client through socket
 *
 * @param  {object} resource that was requested
 * @param  {error|null} error
 * @param  {object} response
 * @param  {string} body
 */
function emitResponse(resource, error, response, body) {
  var timeDiff = Date.now() - resource.startTs;

  if (error) {
    log.debug('[ERROR]: (id: ' + resource.id + ', url: ' + resource.url + ')');
    log.trace(
      '[ERROR]: (id: ' +
        resource.id +
        ', url: ' +
        resource.url +
        ') body : (' +
        error.toString() +
        ')'
    );

    let newResource = Object.assign({}, resource, {
      url: urlHelper.deconstructUrl(this.cdapConfig, resource.url, resource.requestOrigin),
    });
    this.connection.write(
      JSON.stringify(
        {
          resource: newResource,
          error: error,
          warning: error.toString(),
          statusCode: response && response.statusCode,
          response: response && response.body,
        },
        stripResource
      )
    );
  } else {
    log.debug('[SUCCESS]: (id: ' + resource.id + ', url: ' + resource.url + ')');
    log.trace(
      '[' +
        timeDiff +
        'ms] Success (' +
        resource.id +
        ',' +
        resource.url +
        ') body : (' +
        JSON.stringify(body) +
        ')'
    );
    let newResource = Object.assign({}, resource, {
      url: urlHelper.deconstructUrl(this.cdapConfig, resource.url, resource.requestOrigin),
    });
    log.debug('[RESPONSE]: (id: ' + newResource.id + ', url: ' + newResource.url + ')');
    this.connection.write(
      JSON.stringify(
        {
          resource: newResource,
          statusCode: response.statusCode,
          response: body,
        },
        stripResource
      )
    );
  }
}

/**
 * @private onSocketData
 * @param  {string} message received via socket
 */
function onSocketData(message) {
  try {
    message = JSON.parse(message);
    var r = message.resource;
    r.url = urlHelper.constructUrl(
      this.cdapConfig,
      r.url,
      r.requestOrigin || urlHelper.REQUEST_ORIGIN_ROUTER
    );
    switch (message.action) {
      case 'template-config':
        log.debug(
          'ETL application config request (' +
            r.method +
            ',' +
            r.id +
            ',' +
            r.templateid +
            ',' +
            r.pluginid
        );
        this.pushConfiguration(r);
        break;
      case 'request':
        r.startTs = Date.now();
        log.debug('[REQUEST]: (method: ' + r.method + ', id: ' + r.id + ', url: ' + r.url + ')');
        request(r, emitResponse.bind(this, r)).on('error', function(err) {
          log.error('[ERROR]: (url: ' + r.url + ') ' + err.message);
        });
        break;
    }
  } catch (e) {
    log.warn(e);
  }
}

/**
 * @private onSocketClose
 */
function onSocketClose() {
  log.debug('[SOCKET CLOSE] Connection to client "' + this.connection.id + '" closed');
}

export default Aggregator;
