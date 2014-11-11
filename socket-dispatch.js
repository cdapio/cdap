/**
 * send socket msgs to the aggregator
 * a new Dispatch is instantiated for each websocket connection
 */

var hash = require('object-hash'),
    HashTable = hash.HashTable;

HashTable.prototype.remove = function (obj) {
  var key = hash(obj),
      count = this.getCount(key);
  if(count===1) {
    delete this._table[key];
  } else {
    this._table[key].count = count-1;
  }
};


function Dispatch (conn) {
  console.log('socket init', conn.id);

  conn.on('data', this.onSocketData.bind(this));
  conn.on('close', this.onSocketClosed.bind(this));

  this.connection = conn;
  this.polledResources = new HashTable();
}


Dispatch.prototype.onSocketData = function (message) {
  try {
    message = JSON.parse(message);
    console.log('socket data', conn.id, message);

    switch(message.action) {

      case 'poll-start':
        this.polledResources.add(message.resource);
        /* intentional fall-through */

      case 'fetch':
        conn.write(JSON.stringify({
          response: [ Date.now(), Math.random() ],
          resource: message.resource
        }));
        break;

      case 'poll-stop':
        this.polledResources.remove(message.resource);
        break;
    }


  }
  catch (e) {
    console.error(e);
  }
};

Dispatch.prototype.onSocketClosed = function () {
  console.log('socket closed', this.connection.id);
  this.polledResources.reset();
};

module.exports = Dispatch;

