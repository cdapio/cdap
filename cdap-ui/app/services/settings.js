/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

angular.module(PKG.name + '.services')

  .factory('mySettings', function (MyPersistentStorage) {
    return new MyPersistentStorage('user');
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

      myHelpers.deepSet(this.data, key, value);

      return data.request(
        {
          method: 'PUT',
          _cdapPath: this.endpoint,
          body: this.data
        }
      );

    };


    /**
     * retrieve a preference
     * @param {string} key
     * @param {boolean} force true to bypass cache
     * @return {promise} resolved with the value
     */
    MyPersistentStorage.prototype.get = function (key, force) {

      var val = myHelpers.deepGet(this.data, key, true);

      if (!force && val) {
        return $q.when(val);
      }

      var self = this;

      if (this.pending) {
        var deferred = $q.defer();
        this.pending.promise.then(function () {
          deferred.resolve(
            myHelpers.deepGet(self.data, key, true)
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
            myHelpers.deepGet(self.data, key, true)
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
