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
  .service('StatusFactory', function($http, EventPipe, myAuth, $rootScope, MYAUTH_EVENT, MY_CONFIG, $timeout) {

    var isLoggedIn = true;
    this.startPolling = function () {
      if (isLoggedIn){
        beginPolling.bind(this)();
      }
    };
    this.stopPolling = function() {
      isLoggedIn = false;
    };
    $rootScope.$on(MYAUTH_EVENT.logoutSuccess, this.stopPolling.bind(this));

    function beginPolling() {

      _.debounce(function() {
        if (isLoggedIn) {
          $http.get(
            (MY_CONFIG.sslEnabled? 'https://': 'http://') + window.location.host + '/backendstatus',
            {ignoreLoadingBar: true}
          )
               .success(success.bind(this))
               .error(error.bind(this));
        }
      }.bind(this), 2000)();

    }

    function success() {
      EventPipe.emit('backendUp');
      this.startPolling();
    }

    function error(err) {
      if (!reAuthenticate(err)) {
        this.startPolling();
        EventPipe.emit('backendDown');
      }
    }



    function reAuthenticate(err) {
      if (angular.isObject(err) && err.auth_uri) {
        $timeout(function() {
          EventPipe.emit('backendUp');
          myAuth.logout();
        });
        return true;
      }
      return false;
    }

    // this.startPolling();
  });
