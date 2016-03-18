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
  .controller('MySearchCtrl', function($stateParams, myTagsApi, $timeout, $scope) {
    var vm = this;

    vm.showSearchBox = false;
    vm.searchTerm = '';
    vm.searchResults = [];

    vm.showSearch = function () {
      vm.showSearchBox = true;
      $timeout(function () {
        angular.element(document.getElementById('global-search'))[0].focus();
      });
    };

    vm.onBlur = function () {
      vm.showSearchBox = false;
      vm.searchTerm = '';
      vm.searchResults = [];
    };

    vm.search = function (event) {
      event.stopPropagation();
      if (!vm.searchTerm.length || event.keyCode !== 13) { return; }

      var params = {
        namespaceId: $stateParams.namespace || $stateParams.nsadmin,
        query: vm.searchTerm
      };
      myTagsApi.searchTags(params)
        .$promise
        .then(function (res) {

          var parsedSearch = [];
          angular.forEach(res, function (entity) {
            parsedSearch.push(parseEntity(entity.entityId));
          });

          vm.searchResults = parsedSearch;
        });

    };

    $scope.$on('$stateChangeSuccess', vm.onBlur);


    function parseEntity(entityObj) {
      switch (entityObj.type) {
        case 'stream':
          return {
            name: entityObj.id.streamName,
            stateParams: {
              namespaceId: entityObj.id.namespace.id,
              streamId: entityObj.id.streamName,
            },
            type: 'Stream',
            icon: 'icon-streams'
          };
        case 'datasetinstance':
          return {
            name: entityObj.id.instanceId,
            stateParams: {
              namespaceId: entityObj.id.namespace.id,
              datasetId: entityObj.id.instanceId,
            },
            type: 'Dataset',
            icon: 'icon-datasets'
          };
        case 'application':
          return {
            name: entityObj.id.applicationId,
            stateParams: {
              namespaceId: entityObj.id.namespace.id,
              appId: entityObj.id.applicationId,
            },
            type: 'App',
            icon: 'icon-app'
          };
        case 'program':
          return {
            name: entityObj.id.id,
            stateParams: {
              namespaceId: entityObj.id.application.namespace.id,
              appId: entityObj.id.application.applicationId,
              programId: entityObj.id.id,
            },
            programType: entityObj.id.type,
            type: entityObj.id.type,
            icon: (entityObj.id.type.toLowerCase() === 'flow'? 'icon-tigon' : 'icon-' + entityObj.id.type.toLowerCase())
          };
        default:
          return;
      }
    }

  });
