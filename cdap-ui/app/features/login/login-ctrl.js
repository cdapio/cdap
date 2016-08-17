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
 * Login Ctrl
 */

angular.module(PKG.name+'.feature.login').controller('LoginCtrl',
function ($scope, myAuth, myAlert, $state, cfpLoadingBar, $timeout,
   MYAUTH_EVENT, MY_CONFIG, caskFocusManager, myLoadingService) {

  $scope.credentials = myAuth.remembered();
  $scope.submitting = false;

  $scope.isAuthenticated = MY_CONFIG.securityEnabled || myAuth.isAuthenticated();

  $scope.doLogin = function (c) {
    $scope.submitting = true;
    myLoadingService.showLoadingIcon();
    cfpLoadingBar.start();

    myAuth.login(c)
      .finally(function() {
        $scope.submitting = false;
        myLoadingService.hideLoadingIcon();
        cfpLoadingBar.complete();
      });
  };

  $scope.$on('$viewContentLoaded', function() {
    if(MY_CONFIG.securityEnabled) {
      focusLoginField();
    }
  });

  $scope.$on(MYAUTH_EVENT.loginFailed, function (event, err) {
    focusLoginField();
    $scope.error = err.statusCode === 401 ? 'Login failed. Invalid username or password' : err.data;
  });

  /* ----------------------------------------------------------------------- */

  function focusLoginField() {
    $timeout(function() {
      caskFocusManager.select($scope.credentials.username ? 'password' : 'username');
    }, 10); // the addtl timeout is so this triggers AFTER any potential focus() on an $alert
  }

});
