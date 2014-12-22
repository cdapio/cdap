/*global require, module */

var _ = require('lodash'),
    request = require('request'),
    colors = require('colors/safe'),
    HashTable = require('./hashtable.js');


var POLL_INTERVAL = 1000;

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
  this.polledResources = new HashTable();
  this.bodyCache = {};

  this.log('init');
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
 * schedule polling
 */
Aggregator.prototype.planPolling = function () {
  this.timeout = setTimeout(_.bind(doPoll, this), POLL_INTERVAL);
};

/**
 * stop polling
 */
Aggregator.prototype.stopPolling = function () {
  clearTimeout(this.timeout);
  this.timeout = null;
};


/**
 * @private doPoll
 * requests all the polled resources
 */
function doPoll () {
  var that = this,
      rscs = this.polledResources.toArray(),
      pollAgain = _.after(rscs.length, _.bind(this.planPolling, this));

  this.log('poll', rscs.length);
  _.forEach(rscs, function(one){
    var resource = one.value, k = one.hash;
    request(resource, function(error, response, body){

      if(error || _.isEqual(that.bodyCache[one.hash], body)) {
        that.log('not emitting', resource.url);
        return; // we do not send down identical bodies
      }

      that.bodyCache[one.hash] = body;
      emitResponse.call(that, resource, false, response, body);

    }).on('response', pollAgain);
  });
}




/**
 * @private emitResponse
 *
 * sends data back to the client through socket
 * TODO: only send it down if it changed
 *
 * @param  {object} resource that was requested
 * @param  {error|null} error
 * @param  {object} response
 * @param  {string} body
 */
function emitResponse (resource, error, response, body) {

  if(error) { // still emit a warning
    this.log(resource.url, error);
    this.connection.write(JSON.stringify({
      resource: resource,
      warning: error.toString()
    }));
    return;
  }

  this.log('emit', resource.url);
  this.connection.write(JSON.stringify({
    resource: resource,
    statusCode: response.statusCode,
    response: body
  }));
}

/**
 * @private onSocketData
 * @param  {string} message received via socket
 */
function onSocketData (message) {
  try {
    message = JSON.parse(message);
    this.log('data', message.action);

    var r = message.resource;
    // @TODO whitelist resources

    switch(message.action) {
      case 'poll-start':
        this.polledResources.add(r);
        if(!this.timeout) {
          this.planPolling();
        }
        /* falls through */
      case 'fetch':
        request(r, _.bind(emitResponse, this, r));
        break;
      // Syntactic sugar.
      // TODO: Reduce redundancy.
      case 'post':
        request(r, _.bind(emitResponse, this, r));
        break;
      case 'poll-stop':
        this.polledResources.remove(r);
        if(!Object.keys(this.polledResources.table()).length) {
          this.stopPolling();
        }
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
  this.log('closed');
  this.stopPolling();
  this.polledResources.reset();
}


module.exports = Aggregator;
