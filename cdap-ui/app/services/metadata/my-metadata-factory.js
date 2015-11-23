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
          deferred.resolve(res);
        }, function (err) {
          deferred.reject(err);
        });

      return deferred.promise;
    }

    function getAppsMetadata(params) {

      var deferred = $q.defer();

      myMetadataApi.getAppsMetadata(params)
        .$promise
        .then(function (res) {
          deferred.resolve(res);
        }, function (err) {
          deferred.reject(err);
        });

      return deferred.promise;
    }

    function getDatasetsMetadata(params) {

      var deferred = $q.defer();

      myMetadataApi.getDatasetsMetadata(params)
        .$promise
        .then(function (res) {
          deferred.resolve(res);
        }, function (err) {
          deferred.reject(err);
        });

      return deferred.promise;
    }

    function getStreamsMetadata(params) {

      var deferred = $q.defer();

      myMetadataApi.getStreamsMetadata(params)
        .$promise
        .then(function (res) {
          deferred.resolve(res);
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

    function addAppsMetadata(tag, params) {
      var tagName = [tag];

      var deferred = $q.defer();

      myMetadataApi.setAppsMetadata(params, tagName)
        .$promise
        .then(function () {
          return myMetadataApi.getAppsMetadata(params).$promise;
        })
        .then(function (res) {
          deferred.resolve(res);
        });

      return deferred.promise;
    }

    function addDatasetsMetadata(tag, params) {
      var tagName = [tag];

      var deferred = $q.defer();

      myMetadataApi.setDatasetsMetadata(params, tagName)
        .$promise
        .then(function () {
          return myMetadataApi.getDatasetsMetadata(params).$promise;
        })
        .then(function (res) {
          deferred.resolve(res);
        });

      return deferred.promise;
    }

    function addStreamsMetadata(tag, params) {
      var tagName = [tag];

      var deferred = $q.defer();

      myMetadataApi.setStreamsMetadata(params, tagName)
        .$promise
        .then(function () {
          return myMetadataApi.getStreamsMetadata(params).$promise;
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

    function deleteAppsMetadata(tag, params) {
      var deleteParams = angular.extend({tag: tag}, params);

      var deferred = $q.defer();

      myMetadataApi.deleteAppsMetadata(deleteParams)
        .$promise
        .then(function () {
          return myMetadataApi.getAppsMetadata(params).$promise;
        })
        .then(function (res) {
          deferred.resolve(res);
        });

      return deferred.promise;
    }

    function deleteDatasetsMetadata(tag, params) {
      var deleteParams = angular.extend({tag: tag}, params);

      var deferred = $q.defer();

      myMetadataApi.deleteDatasetsMetadata(deleteParams)
        .$promise
        .then(function () {
          return myMetadataApi.getDatasetsMetadata(params).$promise;
        })
        .then(function (res) {
          deferred.resolve(res);
        });

      return deferred.promise;
    }

    function deleteStreamsMetadata(tag, params) {
      var deleteParams = angular.extend({tag: tag}, params);

      var deferred = $q.defer();

      myMetadataApi.deleteStreamsMetadata(deleteParams)
        .$promise
        .then(function () {
          return myMetadataApi.getStreamsMetadata(params).$promise;
        })
        .then(function (res) {
          deferred.resolve(res);
        });

      return deferred.promise;
    }

    return {
      getProgramMetadata: getProgramMetadata,
      getAppsMetadata: getAppsMetadata,
      getStreamsMetadata: getStreamsMetadata,
      getDatasetsMetadata: getDatasetsMetadata,

      addProgramMetadata: addProgramMetadata,
      deleteProgramMetadata: deleteProgramMetadata,

      addAppsMetadata: addAppsMetadata,
      deleteAppsMetadata: deleteAppsMetadata,

      addDatasetsMetadata: addDatasetsMetadata,
      deleteDatasetsMetadata: deleteDatasetsMetadata,

      addStreamsMetadata: addStreamsMetadata,
      deleteStreamsMetadata: deleteStreamsMetadata
    };

  });
