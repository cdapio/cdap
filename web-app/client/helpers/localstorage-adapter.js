/**
 * Local storage adapter for Ember.
 * Stores data in local storage. Maintains an index of values for a quick lookup of existing
 * values.
 */

define([], function () {

  var DEFAULT_INTERVAL = 1000;

  var LSAdapter = Em.Object.extend(Ember.Evented, {

    /**
     * Initializer
     * @param  {string} storeName.
     * @param  {string} stalenessCheckPath Url path to check if cache is stale.
     * @param  {number} interval to check staleness.
     */
    init: function (storeName, stalenessCheckPath, interval) {
      var self = this;
      this.tokenMap = {};
      this.storeName = storeName;
      this.interval = null;
      localStorage.setItem(storeName, JSON.stringify({}));
      
      if (stalenessCheckPath) {
        var interval = interval || DEFAULT_INTERVAL;
        self.stalenessChecker(stalenessCheckPath, interval);
      }
      
    },

    /**
     * Checks staleness of cache based on a resource that determines staleness.
     * @param  {string} path url path.
     * @param  {number} interval between checks.
     */
    stalenessChecker: function (path, interval) {
      var self = this;
      self.interval = setInterval(function () {
        var jqXhr = $.get(path).done(function (data) {
          var storedValue = self.find(path);
          if (!storedValue) {
            self.save(path, data);
          } else {
            // Cached data does not match fresh data.
            if (storedValue !== data) {
              self.clear();

              //Trigger event to let listeners know cache has expired.
              self.trigger('cacheExpired');
            }
          }
        }); 
      }, interval);
    },

    /**
     * Saves record to cache, overwrites existing values.
     * @param  {string} recordName.
     * @param  {?} value any value.
     */
    save: function (recordName, value) {
      var storage = JSON.parse(localStorage.getItem(this.storeName));
      var id = this.generateId(recordName);
      storage[id] = value;
      localStorage.setItem(this.storeName, JSON.stringify(storage));
    },

    /**
     * Checks for and finds record in cache.
     * @param  {string} recordName.
     * @return {null|?} return record retrived from cache.
     */
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

    /**
     * Clears cache and index.
     */
    clear: function () {
      this.tokenMap = {};
      localStorage.setItem(this.storeName, JSON.stringify({}));
    },

    /**
     * Looks up unique id for a record or generates it and adds it to index.
     * @param  {string} recordName.
     * @return {string} id unique id for record.
     */
    generateId: function (recordName) {
      var id = this.tokenMap[recordName] || Math.random().toString(36).substr(2,9);
      this.tokenMap[recordName] = id;
      return id;
    }

  });

  return LSAdapter;

});