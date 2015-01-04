angular.module(PKG.name + '.services')
  .service('myPrefStore', function myPrefStore($q, MyDataSource) {

    this.preferences = {};

    var self = this,
        data = new MyDataSource(),
        queryInProgress = null,
        RESOURCE = {
          _cdapNsPath: '/preferences'
        };

    /**
     * set a preference
     * @param {string} key
     * @param {mixed} value
     * @return {promise} resolved with the value
     */
    this.setPref = function (key, value) {

      var deferred = $q.defer();

      data.request(
        angular.extend(
          { method: 'PUT', body: value },
          RESOURCE
        ),
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
    this.getPref = function (key, force) {
      if (!force && this.preferences[key]) {
          return $q.when(this.preferences[key]);
      }

      if (queryInProgress) {
        var deferred = $q.defer();
        queryInProgress.promise.then(function () {
          deferred.resolve(self.preferences[key]);
        });
        return deferred.promise;
      }

      queryInProgress = $q.defer();

      data.request(
        angular.extend(
          { method: 'GET' },
          RESOURCE
        ),
        function (res) {
          self.preferences = res;
          queryInProgress.resolve(res[key]);
        }
      );

      queryInProgress.promise.finally(function () {
        queryInProgress = null;
      });

      return queryInProgress.promise;
    };
});
