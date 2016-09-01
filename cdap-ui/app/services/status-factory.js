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

    var isPollingEnabled = true;
    this.startPolling = function () {
      if (isPollingEnabled){
        beginPolling.bind(this)();
      }
    };
    this.stopPolling = function() {
      isPollingEnabled = false;
    };

    function beginPolling() {

      _.debounce(function() {
        if (isPollingEnabled) {
          $http.get(
            '/backendstatus',
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

    function error(err, statusCode) {
      if (err && err.code === 'ECONNREFUSED') {
        this.startPolling();
        EventPipe.emit('backendDown', 'Unable to connect to CDAP Router', 'Attempting to connect...');
        return;
      } else if (statusCode === 503 || statusCode === 500) {
        this.startPolling();
        EventPipe.emit('backendDown');
      }
      reAuthenticate(statusCode);
    }

    function reAuthenticate(statusCode) {
      if (statusCode === 401 || statusCode === 200) {
        $timeout(function() {
          EventPipe.emit('backendUp');
          myAuth.logout();
        });
        return true;
      }
      return false;
    }

    this.startPolling();
  });
