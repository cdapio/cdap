angular.module(PKG.name + '.services')

  .factory('mySettings', function (MyPersistentStorage) {
    return new MyPersistentStorage('consolesettings');
  })

  .factory('MyPersistentStorage', function MyPersistentStorageFactory($q, MyDataSource, myHelpers) {

    var data = new MyDataSource();

    function MyPersistentStorage (type) {
      this.endpoint = '/configuration/'+type;

      // our cache of the server-side data
      this.data = {};

      // flag so we dont fire off multiple similar queries
      this.pending = null;
    }

    /**
     * set a preference
     * @param {string} key, can have a path like "foo.bar.baz"
     * @param {mixed} value
     * @return {promise} resolved with the response from server
     */
    MyPersistentStorage.prototype.set = function (key, value) {
      var deferred = $q.defer();

      myHelpers.deepSet(this.data, key, value);

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
    MyPersistentStorage.prototype.get = function (key, force) {

      var val = myHelpers.deepGet(this.data, key);

      if (!force && val) {
        return $q.when(val);
      }

      var self = this;

      if (this.pending) {
        var deferred = $q.defer();
        this.pending.promise.then(function () {
          deferred.resolve(
            myHelpers.deepGet(self.data, key)
          );
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
          self.pending.resolve(
            myHelpers.deepGet(self.data, key)
          );
        }
      );

      var promise = this.pending.promise;

      promise.finally(function () {
        self.pending = null;
      });

      return promise;
    };

    return MyPersistentStorage;
  });
