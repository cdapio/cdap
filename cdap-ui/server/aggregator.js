/*global require, module */

var _ = require('lodash'),
    request = require('request'),
    colors = require('colors/safe');

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
  // make "new" optional
  if ( !(this instanceof Aggregator) ) {
    return new Aggregator(conn);
  }

  conn.on('data', _.bind(onSocketData, this));
  conn.on('close', _.bind(onSocketClose, this));

  this.connection = conn;

  // WebSocket local resource pool. Key here is the resource id
  // as send from the backend. The FE has to guarantee that the
  // the resource id is unique within a websocket connection.
  this.polledResources = {};
}

/**
 * Logs the error message to console.
 */
Aggregator.prototype.log = function () {
  console.log(
    colors.cyan('sock'),
    colors.dim(this.connection.id),
    _(arguments).join(' ')
  );
};

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
  // WARN: This assumes that the browser side ids are unique for a websocket session.
  // This check is needed for Safari.
  if(this.polledResources[resource.id]) {
    console.log("Resource id " + resource.id + " already registered.");
    return;
  }
  resource.interval = resource.interval || POLL_INTERVAL;
  this.polledResources[resource.id] = resource;
  _.bind(doPoll, this, resource)();
}

/**
 * This method is called regularly by 'doPoll' to register the next interval
 * for timeout. Every resource handle has a flag is used to indicate if the
 * the resource has been requested to be stopped, if it's already stopped, then
 * there is nothing for us to do. If it's not then we go ahead and register
 * the interval timeout.
 */
Aggregator.prototype.scheduleAnotherIteration = function (resource) {
  if (resource.stop) {
    // Don't reschedule another iteration if the resource has been stopped
    return;
  }
  // console.log(Object.keys(this.polledResources).length + ':' + Math.floor(Date.now()/1000) +
  //    ': scheduling for: ' + resource.id + ', interval: ' + resource.interval + ' - ' + resource.url);
  resource.timerId = setTimeout(_.bind(doPoll, this, resource), resource.interval);
};

/**
 * Stops the polling of a resource. The resources that has been requested to be stopped
 * is removed from the websocket local poll and the timeouts are cleared and stop flag
 * is set to true.
 */
Aggregator.prototype.stopPolling = function (resource) {
  var thisResource = removeFromObj(this.polledResources, resource.id);
  if (thisResource === undefined) {
    return;
  }
  clearTimeout(thisResource.timerId);
  thisResource.stop = true;
};

/**
 * Iterates through the websocket local resources and clears the timers and sets
 * the flag. This is called when the websocket is closing the connection.
 */
Aggregator.prototype.stopPollingAll = function() {
  for (var id in this.polledResources) {
    var resource = this.polledResources[id];
    clearTimeout(resource.timerId);
    resource.stop = true;
  }
}

/**
 * Removes the resource id from the websocket connection local resource pool.
 */
function removeFromObj(obj, key) {
  var el = obj[key];
  delete obj[key];
  return el;
}

/**
 * 'doPoll' is the doer - it makes the resource request call to backend and
 * sends the response back once it receives it. Upon completion of the request
 * it schedulers the interval for next trigger.
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
    var r = message.resource;

    switch(message.action) {
      case 'poll-start':
        this.startPolling(r);
        break;
      case 'request':
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
  this.polledResources = {};
}

module.exports = Aggregator;
