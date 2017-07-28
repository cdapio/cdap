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
  .service('myNamespace', function myNamespace($q, MyCDAPDataSource, EventPipe, $http, $rootScope, myAuth, myHelpers, $state) {

    this.namespaceList = [];
    window.CaskCommon.StatusFactory.startPollingForBackendStatus();
    var data = new MyCDAPDataSource(),
        prom,
        queryInProgress = null;


    this.getList = function (force) {
      if (!force && this.namespaceList.length) {
          return $q.when(this.namespaceList);
      }

      if (!queryInProgress) {
        prom = $q.defer();
        queryInProgress = true;
        data.request(
          {
            _cdapPath: '/namespaces',
            method: 'GET'
          })
            .then(
              (function(res) {

                if (!res.length && !$state.includes('admin.**')) {
                  $state.go('unauthorized');
                }

                this.namespaceList = res;
                EventPipe.emit('namespace.update');
                prom.resolve(res);
                queryInProgress = null;
              }).bind(this),
              function (err) {
                prom.reject(err);
                queryInProgress = null;
              }
            );
      }

      return prom.promise;
    };

    this.getDisplayName = function(name) {
      var ns = this.namespaceList.filter(function(namespace) {
        return namespace.name === name;
      });
      return ns[0].name || name;
    };

  });
