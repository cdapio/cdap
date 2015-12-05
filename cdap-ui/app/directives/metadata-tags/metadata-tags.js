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

angular.module(PKG.name + '.commons')
  .directive('myMetadataTags', function () {

    return {
      restrict: 'E',
      controller: 'MetadataTagsController',
      controllerAs: 'MetadataController',
      scope: {
        params: '=',
        type: '@',
        tagLimit: '='
      },
      templateUrl: 'metadata-tags/metadata-tags.html',
    };
  })
  .controller('MetadataTagsController', function ($scope, myMetadataFactory, caskFocusManager, $state) {
    this.metadataAddOpen = false;
    this.metadataTags = [];

    this.tagLimit = $scope.tagLimit;
    this.limit = $scope.tagLimit;

    var prom;
    switch($scope.type) {
      case 'datasets':
        prom = myMetadataFactory.getDatasetsMetadata($scope.params);
        break;
      case 'apps':
        prom = myMetadataFactory.getAppsMetadata($scope.params);
        break;
      case 'programs':
        prom = myMetadataFactory.getProgramMetadata($scope.params);
        break;
      case 'streams':
        prom = myMetadataFactory.getStreamsMetadata($scope.params);
        break;
    }

    prom.then(function (res) {
      this.metadataTags = res;
    }.bind(this));

    this.addMetadata = function () {
      var prom;
      switch($scope.type) {
        case 'datasets':
          prom = myMetadataFactory.addDatasetsMetadata(this.tag, $scope.params);
          break;
        case 'apps':
          prom = myMetadataFactory.addAppsMetadata(this.tag, $scope.params);
          break;
        case 'programs':
          prom = myMetadataFactory.addProgramMetadata(this.tag, $scope.params);
          break;
        case 'streams':
          prom = myMetadataFactory.addStreamsMetadata(this.tag, $scope.params);
          break;
      }
      prom.then(function (res) {
        this.metadataTags = res;
        this.tag = '';
      }.bind(this));
    };

    this.deleteMetadata = function (event, tag) {
      event.stopPropagation();
      var prom;
      switch($scope.type) {
        case 'datasets':
          prom = myMetadataFactory.deleteDatasetsMetadata(tag, $scope.params);
          break;
        case 'apps':
          prom = myMetadataFactory.deleteAppsMetadata(tag, $scope.params);
          break;
        case 'programs':
          prom = myMetadataFactory.deleteProgramMetadata(tag, $scope.params);
          break;
        case 'streams':
          prom = myMetadataFactory.deleteStreamsMetadata(tag, $scope.params);
          break;
      }
      prom.then(function (res) {
        this.metadataTags = res;
      }.bind(this));
    };

    this.escapeMetadata = function () {
      this.tag = '';
      this.metadataAddOpen = false;
    };

    this.goToTag = function(event, tag) {
      event.stopPropagation();
      $state.go('search.objectswithtags', {tag: tag});
    };

    caskFocusManager.focus('metadataInput');

  });
