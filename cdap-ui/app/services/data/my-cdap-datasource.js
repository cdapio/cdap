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
  .factory('MyCDAPDataSource', function(MyDataSource, $rootScope, myCdapUrl, $cookies) {
    function MyCDAPDataSource(scope) {
      scope = scope || $rootScope.$new();

      if (!(this instanceof MyCDAPDataSource)) {
        return new MyCDAPDataSource(scope);
      }

      this.MyDataSource = new MyDataSource(scope);
    }

    MyCDAPDataSource.prototype.poll = function (resource, cb, errorCb) {

      // FIXME: There is a circular dependency and that is why
      // myAuth.isAuthenticated is not used. There should be a better way to do this.
      if (window.CDAP_CONFIG.securityEnabled && $cookies.get('CDAP_Auth_Token')) {
        resource.headers = {
          Authorization: 'Bearer ' + $cookies.get('CDAP_Auth_Token')
        };
      } else if (
        window.CaskCommon.CDAPHelpers.isAuthSetToManagedMode() &&
        $rootScope.currentUser &&
        $rootScope.currentUser.token
      ) {
        resource.headers = {
          Authorization: 'Bearer ' + $rootScope.currentUser.token
        };
      } else {
        resource.headers = {};
      }

      if (!resource.url) {
        resource.url = myCdapUrl.constructUrl(resource);
      }

      return this.MyDataSource.poll(resource, cb, errorCb);
    };

    MyCDAPDataSource.prototype.stopPoll = function (resourceId) {
      return this.MyDataSource.stopPoll(resourceId);
    };

    MyCDAPDataSource.prototype.config = function(resource, cb, errorCb) {
      resource.actionName = 'template-config';
      return this.MyDataSource.config(resource, cb, errorCb);
    };

    MyCDAPDataSource.prototype.request = function(resource, cb, errorCb) {
      if (window.CDAP_CONFIG.securityEnabled && $cookies.get('CDAP_Auth_Token')) {
        resource.headers = {
          Authorization: 'Bearer ' + $cookies.get('CDAP_Auth_Token')
        };
      } else if (
        window.CaskCommon.CDAPHelpers.isAuthSetToManagedMode() &&
        $rootScope.currentUser &&
        $rootScope.currentUser.token
      ) {
        resource.headers = {
          Authorization: 'Bearer '+ $rootScope.currentUser.token
        };
      } else {
        resource.headers = {};
      }

      if (!resource.url) {
        resource.url = myCdapUrl.constructUrl(resource);
      }

      return this.MyDataSource.request(resource, cb, errorCb);
    };

    return MyCDAPDataSource;

  });
