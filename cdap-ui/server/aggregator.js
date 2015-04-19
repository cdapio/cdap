/*global require, module */

var _ = require('lodash'),
    request = require('request'),
    colors = require('colors/safe'),
    HashTable = require('object-hash').HashTable; // TODO: remove this dependency


var POLL_INTERVAL = 3000;

/**
 * Aggregator
 * receives resourceObj, aggregate them,
 * and send poll responses back through socket
 *
 * @param {Object} SockJS connection
 */
function Aggregator (conn) {
  // make "new" optional
  if ( !(this instanceof Aggregator) ) {
    return new Aggregator(conn);
  }

  conn.on('data', _.bind(onSocketData, this));
  conn.on('close', _.bind(onSocketClose, this));

  this.connection = conn;
  this.polledResources = {};

  // this.log('init');
}

/**
 * log something
 */
Aggregator.prototype.log = function () {
  console.log(
    colors.cyan('sock'),
    colors.dim(this.connection.id),
    _(arguments).join(' ')
  );
};

/**
 * Polls immediately and schedules a timer to be trigger
 */
Aggregator.prototype.startPolling = function (resource) {
  console.log('registering ID: ' + resource.id);
  resource.interval = resource.interval || POLL_INTERVAL;
  this.polledResources[resource.id] = resource;
  _.bind(doPoll, this, resource)();
}

/**
 * schedule polling
 */
Aggregator.prototype.scheduleAnotherIteration = function (resource) {
  if (resource.stop) {
    // Don't reschedule another iteration if the resource has been stopped
    return;
  }
  console.log(Object.keys(this.polledResources).length + ':' + Math.floor(Date.now()/1000) + ': scheduling for: ' + resource.id + ', interval: ' + resource.interval + ' - ' + resource.url);
  resource.timerId = setTimeout(_.bind(doPoll, this, resource), resource.interval);
};

/**
 * stop polling
 */
Aggregator.prototype.stopPolling = function (resource) {
  console.log('Stopped polling:' + resource.id);
  var thisResource = removeFromObj(this.polledResources, resource.id);
  if (thisResource === undefined) {
    console.log('ERROR' + thisResource);
    return;
  }
  clearTimeout(thisResource.timerId);
  thisResource.stop = true;
};

Aggregator.prototype.stopPollingAll = function() {
  for (var id in this.polledResources) {
    var resource = this.polledResources[id];
    clearTimeout(resource.timerId);
    resource.stop = true;
  }
  this.polledResources = {};
}

function removeFromObj(obj, key) {
  var el = obj[key];
  delete obj[key];
  return el;
}


/**
 * @private doPoll
 * requests all the polled resources
 */
function doPoll (resource) {
    var that = this,
        callBack = _.bind(this.scheduleAnotherIteration, that, resource);

    request(resource, function(error, response, body) {
      if (error) {
        emitResponse.call(that, resource, error);
        return;
      }

      emitResponse.call(that, resource, false, response, body);

    }).on('response', callBack)
    .on('error', callBack);
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
  resource.timerId = undefined;
  resource.stop = undefined;
  if(error) { // still emit a warning
    this.log(resource.url, error);
    this.connection.write(JSON.stringify({
      resource: resource,
      error: error,
      warning: error.toString()
    }));

  } else {

    // this.log('emit', resource.url);
    this.connection.write(JSON.stringify({
      resource: resource,
      statusCode: response.statusCode,
      response: body
    }));
  }
}

/**
 * @private onSocketData
 * @param  {string} message received via socket
 */
function onSocketData (message) {
  try {
    message = JSON.parse(message);
    // this.log('data', message.action);

    var r = message.resource;
    // @TODO whitelist resources

    switch(message.action) {
      case 'poll-start':
        this.startPolling(r);
        break;
      case 'request':
        console.log('requesting: ' + r.id + ' ' + r.url);
        request(r, _.bind(emitResponse, this, r));
        break;
      case 'poll-stop':
        this.stopPolling(r);
        break;
    }

  }
  catch (e) {
    console.error(e);
  }
}

/**
 * @private onSocketClose
 */
function onSocketClose () {
  this.log('socket closed');
  this.stopPollingAll();
}


module.exports = Aggregator;
