/*global require, module */

var _ = require('lodash'),
    request = require('request'),
    colors = require('colors/safe'),
    hash = require('object-hash'),
    HashTable = hash.HashTable;

// https://github.com/puleos/object-hash/pull/12
HashTable.prototype.remove = function (obj) {
  var key = hash(obj),
      count = this.getCount(key);
  if(count===1) {
    delete this._table[key];
  } else {
    this._table[key].count = count-1;
  }
};


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

  this.log('init');
}

/**
 * log something
 */
Aggregator.prototype.log = function (msg) {
  console.log(colors.cyan('sock'), colors.dim(this.connection.id), msg);
};

/**
 * schedule polling
 */
Aggregator.prototype.planPolling = function () {
  this.timeout = setTimeout(_.bind(doPoll, this), 1000);
};

/**
 * stop polling
 */
Aggregator.prototype.stopPolling = function () {
  clearTimeout(this.timeout);
};


/**
 * @private doPoll
 * requests all the polled resources
 */
function doPoll () {
  var rscs = this.polledResources.toArray(),
      pollAgain = _.after(rscs.length, _.bind(this.planPolling, this));

  _.forEach(rscs, function(one){
    var r = one.value;
    request(r, _.bind(emitResponse, this, r)).on('response', pollAgain);
  }, this);
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
  if(!error) {
    var output = { resource: resource };
    output.response = response.toJSON();
    try {
      output.json = JSON.parse(body);
    }
    catch (e) {}
    this.log('emit', output.resource.url);
    this.connection.write(JSON.stringify(output));
  }
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

