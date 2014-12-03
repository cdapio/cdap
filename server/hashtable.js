/*global require, module */
/**
 * HashTable from object-hash module,
 *    patched to allow removing objects from the table.
 *
 * see https://github.com/puleos/object-hash/pull/12
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

module.exports = HashTable;
