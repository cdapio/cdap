/**
 * Local storage adapter for Ember.
 */

define([], function () {

  var LSAdapter = Em.Object.extend({

    init: function (storeName) {
      this.tokenMap = {};
      this.storeName = storeName;
      localStorage.setItem(storeName, JSON.stringify({}));
    },

    save: function (recordName, value) {
      var storage = JSON.parse(localStorage.getItem(this.storeName));
      var id = this.generateId(recordName);
      storage[id] = value;
      localStorage.setItem(this.storeName, JSON.stringify(storage));
    },

    find: function (recordName) {
      if (recordName in this.tokenMap) {
        var storage = JSON.parse(localStorage.getItem(this.storeName));
        var id = this.generateId(recordName);
        var result;
        if (id in storage && storage.hasOwnProperty(id)) {
          result = storage[id];
        }
        return result;  
      }
      return;
    },

    clear: function () {
      localStorage.setItem(this.storeName, JSON.stringify({}));
    },

    generateId: function (recordName) {
      var id = this.tokenMap[recordName] || Math.random().toString(36).substr(2,9);
      this.tokenMap[recordName] = id;
      return id;
    }

  });

  return LSAdapter;

});