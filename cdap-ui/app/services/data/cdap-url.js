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
  .factory('myCdapUrl', function myCdapUrl($stateParams) {

    function constructUrl(resource) {

      var url;

      if(resource._cdapNsPath) {

        var namespace = $stateParams.namespace;

        if(!namespace) {
          throw new Error('_cdapNsPath requires $stateParams.namespace to be defined');
        }

        resource._cdapPath = [
          '/namespaces/',
          namespace,
          resource._cdapNsPath
        ].join('');
        delete resource._cdapNsPath;
      }

      // further sugar for building absolute url
      if(resource._cdapPath) {
        url = resource._cdapPath;
        delete resource._cdapPath;
      }


      return url;
    }

    return {
      constructUrl: constructUrl
    };
  });
