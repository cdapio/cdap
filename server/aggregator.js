/*global require, module */

var request = require('request'),
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

  conn.on('data', _onSocketData.bind(this));
  conn.on('close', _onSocketClose.bind(this));

  this.connection = conn;
  this.polledResources = new HashTable();

  this.log('init');
}

Aggregator.prototype.log = function (msg) {
  console.log(colors.cyan('sock'), colors.dim(this.connection.id), msg);
};

Aggregator.prototype.planPolling = function () {
  this.timeout = setTimeout(_doPoll.bind(this), 1000);
};

Aggregator.prototype.stopPolling = function () {
  clearTimeout(this.timeout);
};

function _doPoll () {
  console.log('polling...');

  this.planPolling();
}

function _emit (resource, error, response, body) {
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

function _onSocketData (message) {
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
        request(r, _emit.bind(this, r));
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

function _onSocketClose () {
  this.log('closed');
  this.polledResources.reset();
  this.stopPolling();
}


module.exports = Aggregator;

