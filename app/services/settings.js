angular.module(PKG.name + '.services')

  .factory('mySettings', function (MyConfigStore) {
    return new MyConfigStore('consolesettings');
  })

  .factory('MyConfigStore', function MyConfigStoreFactory($q, MyDataSource) {

    var data = new MyDataSource();

    function MyConfigStore (type) {
      this.endpoint = '/configuration/'+type;

      // our cache of the server-side data
      this.data = {};

      // flag so we dont fire off multiple similar queries
      this.pending = null;
    }

    /**
     * set a preference
     * @param {string} key
     * @param {mixed} value
     * @return {promise} resolved with the response from server
     */
    MyConfigStore.prototype.set = function (key, value) {
      var deferred = $q.defer();

      this.data[key] = value;

      data.request(
        {
          method: 'PUT',
          _cdapPath: this.endpoint,
          body: this.data
        },
        deferred.resolve
      );

      return deferred.promise;
    };


    /**
     * retrieve a preference
     * @param {string} key
     * @param {boolean} force true to bypass cache
     * @return {promise} resolved with the value
     */
    MyConfigStore.prototype.get = function (key, force) {
      if (!force && this.data[key]) {
        return $q.when(this.data[key]);
      }

      var self = this;

      if (this.pending) {
        var deferred = $q.defer();
        this.pending.promise.then(function () {
          deferred.resolve(self.data[key]);
        });
        return deferred.promise;
      }

      this.pending = $q.defer();

      data.request(
        {
          method: 'GET',
          _cdapPath: this.endpoint
        },
        function (res) {
          self.data = res.property;
          self.pending.resolve(self.data[key]);
        }
      );

      var promise = this.pending.promise;

      promise.finally(function () {
        self.pending = null;
      });

      return promise;
    };

    return MyConfigStore;
  });
