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

    var vm = this;
    vm.metadataAddOpen = false;
    vm.metadataTags = [];

    vm.tagLimit = $scope.tagLimit;
    vm.limit = $scope.tagLimit;

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

    function processResponse(response) {
      var systemMetadataTags = [];
      angular.forEach(response, function (entity) {
        if (entity.scope === 'SYSTEM') {
          systemMetadataTags = entity.tags.map(function (tag) {
            return {
              name: tag,
              scope: 'SYSTEM'
            };
          });
        } else {
          vm.metadataTags = entity.tags.map(function (tag) {
            return {
              name: tag,
              scope: 'USER'
            };
          });
        }
      });

      vm.metadataTags = vm.metadataTags.concat(systemMetadataTags);
    }

    prom.then(function (res) {
      processResponse(res);
    });

    this.addMetadata = function () {
      var prom;
      switch($scope.type) {
        case 'datasets':
          prom = myMetadataFactory.addDatasetsMetadata(vm.tag, $scope.params);
          break;
        case 'apps':
          prom = myMetadataFactory.addAppsMetadata(vm.tag, $scope.params);
          break;
        case 'programs':
          prom = myMetadataFactory.addProgramMetadata(vm.tag, $scope.params);
          break;
        case 'streams':
          prom = myMetadataFactory.addStreamsMetadata(vm.tag, $scope.params);
          break;
      }
      prom.then(function (res) {
        processResponse(res);
        vm.tag = '';
      });
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
        processResponse(res);
      });
    };

    vm.escapeMetadata = function () {
      vm.tag = '';
      vm.metadataAddOpen = false;
    };

    vm.goToTag = function(event, tag) {
      event.stopPropagation();
      $state.go('search.objectswithtags', {tag: tag});
    };

    caskFocusManager.focus('metadataInput');

  });
