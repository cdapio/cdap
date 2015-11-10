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

/**
 * MyBrowserStorage
 * replicates the MyPersistentStorage API
 *
 * makes it easy to promote state
 * from _session_ to _local_ to _remotely stored_
 */
angular.module(PKG.name + '.services')

  .factory('myLocalStorage', function (MyBrowserStorage) {
    return new MyBrowserStorage('local');
  })

  .factory('mySessionStorage', function (MyBrowserStorage) {
    return new MyBrowserStorage('session');
  })

  .factory('MyBrowserStorage', function MyBrowserStorageFactory($q, $localStorage, $sessionStorage, myHelpers) {

    function MyBrowserStorage (type) {
      this.type = type;
      this.data = type==='local' ? $localStorage : $sessionStorage;
    }

    /**
     * set a value
     * @param {string} key, can have a path like "foo.bar.baz"
     * @param {mixed} value
     * @return {promise} resolved with the response from server
     */
    MyBrowserStorage.prototype.set = function (key, value) {
      if(this.type === 'local') {
        key = PKG.name + '.' + key;
      }
      return $q.when(myHelpers.deepSet(this.data, key, value));
    };

    /**
     * retrieve a value
     * @param {string} key
     * @return {promise} resolved with the value
     */
    MyBrowserStorage.prototype.get = function (key) {
      if(this.type === 'local') {
        key = PKG.name + '.' + key;
      }
      return $q.when(myHelpers.deepGet(this.data, key));
    };

    return MyBrowserStorage;
  });
