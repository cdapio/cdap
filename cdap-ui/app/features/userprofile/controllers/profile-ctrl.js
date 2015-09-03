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

angular.module(PKG.name + '.feature.userprofile')
  .controller('UserProfileController', function($scope, $http, myAlert, myAuth, MY_CONFIG) {
    $scope.reAuthenticated = false;
    $scope.isAuthenticated = MY_CONFIG.securityEnabled;
    $scope.credentials = {
      username: myAuth.currentUser.username
    };
    $scope.readonlyUsername = true;

    $scope.doLogin = function(credentials) {
      $http({
        method: 'POST',
        url: '/accessToken',
        data: {
          profile_view: true,
          username: credentials.username,
          password: credentials.password
        }
      })
        .success(function(res) {
          $scope.token = res.access_token;
          $scope.expirationTime = Date.now() + (res.expires_in * 1000);
          $scope.reAuthenticated = true;
        })
        .error(function() {
          myAlert({
            title: 'User Authentication Error',
            content: 'Either Username or Password is wrong. Please try again',
            type: 'danger'
          });
        });
    };

  });
