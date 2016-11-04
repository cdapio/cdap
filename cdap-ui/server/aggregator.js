/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

var request = require('request'),
    fs = require('fs'),
    log4js = require('log4js');

var log = log4js.getLogger('default');
var hash = require('object-hash');

/**
 * Default Poll Interval used by the backend.
 * We set the default poll interval to high, so
 * if any of the frontend needs faster than this
 * time, then would have to pass in the 'interval'
 * in their request.
 */
var POLL_INTERVAL = 10*1000;

/**
 * Aggregator
 * receives resourceObj, aggregate them,
 * and send poll responses back through socket
 *
 * @param {Object} SockJS connection
 */
function Aggregator (conn) {
  // make 'new' optional
  if ( !(this instanceof Aggregator) ) {
    return new Aggregator(conn);
  }

  conn.on('data', onSocketData.bind(this));
  conn.on('close', onSocketClose.bind(this));

  this.connection = conn;

  // WebSocket local resource pool. Key here is the resource id
  // as send from the backend. The FE has to guarantee that the
  // the resource id is unique within a websocket connection.
  this.polledResources = {};
}

/**
 * Checks if the 'id' received from the client is already registered -- This
 * check was added because for whatever reason 'Safari' was sending multiple
 * requests to backend with same ids. As this is happens only once during
 * the start of poll, it's safe to make this check. The assumption here is
 * that the frontend is sending in unique ids within the websocket session.
 *
 * Upon check if it's not duplicate, we invoke doPoll that would make the
 * first call and set the interval for the timeout.
 */
Aggregator.prototype.startPolling = function (resource) {
  resource.interval = resource.interval || POLL_INTERVAL;
  log.debug('[SCHEDULING]: (id: ' + resource.id + ', url: ' + resource.url + ', interval: ' + resource.interval + ')');
  this.polledResources[resource.id] = {
    resource: resource,
    response: null
  };
  doPoll.bind(this, this.polledResources[resource.id].resource)();
};

/**
 * This method is called regularly by 'doPoll' to register the next interval
 * for timeout. Every resource handle has a flag is used to indicate if the
 * the resource has been requested to be stopped, if it's already stopped, then
 * there is nothing for us to do. If it's not then we go ahead and register
 * the interval timeout.
 */
Aggregator.prototype.scheduleAnotherIteration = function (resource) {
  log.debug('[RESCHEDULING]: (id: ' + resource.id + ',' + resource.url + ', url: ' + resource.interval + ')');
  resource.timerId = setTimeout(doPoll.bind(this, resource), resource.interval);
};

/**
 * Stops the polling of a resource. The resources that has been requested to be stopped
 * is removed from the websocket local poll and the timeouts are cleared and stop flag
 * is set to true.
 */
Aggregator.prototype.stopPolling = function (resource) {
  if (!this.polledResources[resource.id]) {
    return;
  }
  var timerId = this.polledResources[resource.id].timerId;
  delete this.polledResources[resource.id];
  clearTimeout(timerId);
};

/**
 * Iterates through the websocket local resources and clears the timers and sets
 * the flag. This is called when the websocket is closing the connection.
 */
Aggregator.prototype.stopPollingAll = function() {
  var id, resource;
  for (id in this.polledResources) {
    if (this.polledResources.hasOwnProperty(id)) {
      resource = this.polledResources[id].resource;
      log.debug('[POLL-STOP]: (id: ' + resource.id + ', url: ' + resource.url + ')');
      clearTimeout(resource.timerId);
    }
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
  var i, paths = filePaths.length;
  var fileFound = true;

  // Check if the configuration is present within the plugin for a template
  for (i=0; i<paths; i++) {
    try {
      configString = fs.readFileSync(filePaths[i], 'utf8');
      statusCode = 200;
      fileFound = true;
      break;
    } catch(e) {
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
    } catch(e) {
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

  this.connection.write(JSON.stringify({
    resource: resource,
    statusCode: statusCode,
    response: config
  }));
};

function validateSemanticsOfConfigJSON(config) {
  var groups = config.groups.position;
  var groupsMap = config.groups;
  var i, j;
  var isValid = true;
  var fields, fieldsMap;

  for (i=0; i<groups.length; i++) {
    if (!groupsMap[groups[i]] || !isValid) {
      isValid = false;
      break;
    }

    fields = groupsMap[groups[i]].position;
    fieldsMap = groupsMap[groups[i]].fields;

    if (!fields || !fieldsMap) {
      isValid = false;
    } else {
      for (j=0; j<fields.length; j++) {
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
 * 'doPoll' is the doer - it makes the resource request call to backend and
 * sends the response back once it receives it. Upon completion of the request
 * it schedulers the interval for next trigger.
 */
function doPoll (resource) {
    if (!this.polledResources[resource.id]) {
      return;
    }
    var that = this,
        callBack = this.scheduleAnotherIteration.bind(that, resource);

    resource.startTs = Date.now();
    request(resource, function(error, response, body) {
      if (error) {
        emitResponse.call(that, resource, error);
        return;
      } else if (response.statusCode > 299) {
        var errMessage = response.statusCode + ' ' + resource.url;
        emitResponse.call(that, resource, errMessage, response, body);
        return;
      }
      emitResponse.call(that, resource, false, response, body);

    }).on('response', callBack)
    .on('error', callBack);
}

/**
 * Helps avoid sending certain properties to the browser (meta attributes used only in the node server)
 */
function stripResource(key, value) {
  // note that 'stop' is not the stop timestamp, but rather a stop flag/signal (unlike the startTs)
  if (key==='timerId' || key==='startTs' || key==='stop') {
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
function emitResponse (resource, error, response, body) {
  var timeDiff = Date.now()  - resource.startTs;
  var responseHash;

  if(error) {
    log.debug('[ERROR]: (id: ' + resource.id + ', url: ' + resource.url + ')');
    log.trace('[ERROR]: (id: ' + resource.id + ', url: ' + resource.url + ') body : (' + error.toString() + ')');

    if (this.polledResources[resource.id]) {
      responseHash = hash(error || {});
      if (this.polledResources[resource.id].response === responseHash.toString()) {
        return;
      } else {
        this.polledResources[resource.id].response = responseHash.toString();
      }
    }

    this.connection.write(JSON.stringify({
      resource: resource,
      error: error,
      warning: error.toString(),
      statusCode: response && response.statusCode,
      response: response && response.body
    }, stripResource));

  } else {
    log.debug('[SUCCESS]: (id: ' + resource.id + ', url: ' + resource.url + ')');
    log.trace('[' + timeDiff + 'ms] Success (' + resource.id + ',' + resource.url + ') body : (' + JSON.stringify(body) + ')');

    if (this.polledResources[resource.id]) {
      responseHash = hash(body || {});
      if (this.polledResources[resource.id].response === responseHash.toString()){
        // No need to send this to the client as nothing changed.
        return;
      } else {
        this.polledResources[resource.id].response = responseHash;
      }
    }
    log.debug('[RESPONSE]: (id: ' + resource.id + ', url: ' + resource.url + ')' );
    this.connection.write(JSON.stringify({
      resource: resource,
      statusCode: response.statusCode,
      response: body
    }, stripResource));
  }
}

/**
 * @private onSocketData
 * @param  {string} message received via socket
 */
function onSocketData (message) {
  try {
    message = JSON.parse(message);
    var r = message.resource;

    switch(message.action) {
      case 'template-config':
        log.debug('ETL application config request (' + r.method + ',' + r.id + ',' + r.templateid + ',' + r.pluginid);
        this.pushConfiguration(r);
        break;
      case 'poll-start':
        log.debug ('[POLL-START]: (method: ' + r.method + ', id: ' + r.id + ', url: ' + r.url + ')');
        this.startPolling(r);
        break;
      case 'request':
        r.startTs = Date.now();
        log.debug ('[REQUEST]: (method: ' + r.method + ', id: ' + r.id + ', url: ' + r.url + ')');
        request(r, emitResponse.bind(this, r))
          .on('error', function (err) {
            log.error(err);
          });
        break;
      case 'poll-stop':
        log.debug ('[POLL-STOP]: (id: ' + r.id + ', url: ' + r.url + ')');
        this.stopPolling(r);
        break;
    }
  }
  catch (e) {
    log.warn(e);
  }
}

/**
 * @private onSocketClose
 */
function onSocketClose () {
  this.stopPollingAll();
  this.polledResources = {};
}

module.exports = Aggregator;
