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

var module = angular.module(PKG.name+'.services');

/*
  inspired by https://medium.com/opinionated-angularjs/
    techniques-for-authentication-in-angularjs-applications-7bbf0346acec
 */

module.constant('MYAUTH_EVENT', {
  loginSuccess: 'myauth-login-success',
  loginFailed: 'myauth-login-failed',
  logoutSuccess: 'myauth-logout-success',
  sessionTimeout: 'myauth-session-timeout',
  notAuthenticated: 'myauth-not-authenticated',
  notAuthorized: 'myauth-not-authorized'
});


module.constant('MYAUTH_ROLE', {
  all: '*',
  user: 'user',
  admin: 'admin'
});


module.service('myAuth', function myAuthService (
  MYAUTH_EVENT,
  MyAuthUser,
  myAuthPromise,
  $rootScope,
  $localStorage,
  $cookies
) {

  /**
   * private method to sync the user everywhere
   */
  var persist = angular.bind(this, function (u) {
    this.currentUser = u;
    $rootScope.currentUser = u;
  });

  this.getUsername = function() {
    if (angular.isObject(this.currentUser)) {
      return this.currentUser.username;
    }
    return false;
  };
  /**
   * remembered
   * @return {object} credentials
   */
  this.remembered = function () {
    var r = $localStorage.remember;
    return angular.extend({
      remember: !!r
    }, r || {});
  };

  /**
   * logout
   */
  this.logout = function () {
    if (this.currentUser){
      persist(null);
      $cookies.remove('CDAP_Auth_Token', {path: '/'});
      $cookies.remove('CDAP_Auth_User', {path: '/'});
      $rootScope.$broadcast(MYAUTH_EVENT.logoutSuccess);
    }
  };

  /**
   * is there someone here?
   * @return {Boolean}
   */
  this.isAuthenticated = function () {
    if (window.CaskCommon.CDAPHelpers.isAuthSetToProxyMode()) {
      return true;
    }
    if (window.CaskCommon.CDAPHelpers.isAuthSetToManagedMode() && this.currentUser) {
      return !!this.currentUser;
    }

    return this.updateCredentialsFromCookie();
  };

  this.updateCredentialsFromCookie = function() {
    if ($cookies.get('CDAP_Auth_Token') && $cookies.get('CDAP_Auth_User')) {
      var user = new MyAuthUser({
        access_token: $cookies.get('CDAP_Auth_Token'),
        username: $cookies.get('CDAP_Auth_User')
      });
      persist(user);
      return !!this.currentUser;
    }
  };

  this.updateCredentialsFromCookie();

});


module.factory('myAuthPromise', function myAuthPromiseFactory (MY_CONFIG, $q, $http) {
  return function myAuthPromise (credentials) {
    var deferred = $q.defer();

    if(MY_CONFIG.securityEnabled) {

      $http({
        url: '/login',
        method: 'POST',
        data: credentials
      })
      .success(function (data) {
        deferred.resolve(angular.extend(data, {
          username: credentials.username
        }));
      })
      .error(function (data, status) {
        deferred.reject({
          data: data,
          statusCode: status
        });
      });

    } else {

      console.warn('Security is disabled, logging in automatically');
      deferred.resolve({
        username: credentials.username
      });

    }

    return deferred.promise;
  };
});


module.factory('MyAuthUser', function MyAuthUserFactory (MYAUTH_ROLE) {

  /**
   * Constructor for currentUser data
   * @param {object} user data
   */
  function User(data) {
    this.token = data.access_token;
    this.username = data.username;
    this.role = MYAUTH_ROLE.user;

    if (data.username==='admin') {
      this.role = MYAUTH_ROLE.admin;
    }
  }


  /**
   * do i haz one of given roles?
   * @param  {String|Array} authorizedRoles
   * @return {Boolean}
   */
  User.prototype.hasRole = function(authorizedRoles) {
    if(this.role === MYAUTH_ROLE.admin) {
      return true;
    }
    if (!angular.isArray(authorizedRoles)) {
      authorizedRoles = [authorizedRoles];
    }
    return authorizedRoles.indexOf(this.role) !== -1;
  };


  /**
   * Omits secure info (i.e. token) and gets object for use
   * in localstorage.
   * @return {Object} storage info.
   */
  User.prototype.storable = function () {
    return {
      username: this.username
    };
  };


  return User;
});
