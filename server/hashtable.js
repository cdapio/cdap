/*global require, module */

var hash = require('object-hash'),
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

module.exports = HashTable;
