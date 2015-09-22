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

angular.module(PKG.name+'.services')
  .factory('myMetadataFactory', function(myMetadataApi, $q){

    function getProgramMetadata(params) {

      var deferred = $q.defer();

      myMetadataApi.getProgramMetadata(params)
        .$promise
        .then(function (res) {
          var metadataTags = res.map(function (tag) {
            return {
              tagName: tag,
              isHover: false
            };
          });

          deferred.resolve(metadataTags);
        }, function (err) {
          deferred.reject(err);
        });

      return deferred.promise;
    }

    function addProgramMetadata(tag, params) {
      var tagName = [tag];

      var deferred = $q.defer();

      myMetadataApi.setProgramMetadata(params, tagName)
        .$promise
        .then(function () {
          return getProgramMetadata(params);
        })
        .then(function (res) {
          deferred.resolve(res);
        });

      return deferred.promise;
    }

    function deleteProgramMetadata(tag, params) {
      var deleteParams = angular.extend({tag: tag}, params);

      var deferred = $q.defer();

      myMetadataApi.deleteProgramMetadata(deleteParams)
        .$promise
        .then(function () {
          return getProgramMetadata(params);
        })
        .then(function (res) {
          deferred.resolve(res);
        });

      return deferred.promise;
    }

    return {
      getProgramMetadata: getProgramMetadata,
      addProgramMetadata: addProgramMetadata,
      deleteProgramMetadata: deleteProgramMetadata
    };

  });
