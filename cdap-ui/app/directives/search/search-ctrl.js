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
  .controller('MySearchCtrl', function($stateParams, myTagsApi, $scope, $document, caskFocusManager) {
    var vm = this;

    vm.showSearchBox = false;
    vm.searchTerm = '';
    vm.searchResults = [];
    vm.searchEntered = false;
    vm.loading = false;

    vm.showSearch = function () {
      vm.showSearchBox = true;
      caskFocusManager.focus('globalSearch');
    };

    vm.onBlur = function () {
      vm.showSearchBox = false;
      vm.searchEntered = false;
      vm.loading = false;
      vm.searchTerm = '';
      vm.searchResults = [];
    };

    vm.search = function (event) {
      event.stopPropagation();
      if (event.keyCode === 27) {
        vm.onBlur();
      }

      if (!vm.searchTerm.length || event.keyCode !== 13) { return; }

      vm.searchEntered = true;
      vm.loading = true;
      var params = {
        namespaceId: $stateParams.namespace || $stateParams.nsadmin,
        query: vm.searchTerm
      };
      myTagsApi.searchTags(params)
        .$promise
        .then(function (res) {
          vm.loading = false;

          var result = res.results.slice(0, 20);

          var parsedSearch = [];
          angular.forEach(result, function (entity) {
            var parsedData = parseEntity(entity.entityId);

            // Checking the return value is something we can display
            if (parsedData) {
              parsedSearch.push(parsedData);
            }
          });

          vm.searchResults = parsedSearch;
        });

    };

    $scope.$on('$stateChangeSuccess', vm.onBlur);

    // trigger on pressing keyboard '?'
    $document.bind('keypress', function (event) {
      if (event.keyCode === 63) {
        vm.showSearch();
      }
    });


    function parseEntity(entityObj) {
      switch (entityObj.type) {
        case 'stream':
          return {
            name: entityObj.id.streamName,
            stateParams: {
              namespaceId: entityObj.id.namespace.id,
              streamId: entityObj.id.streamName,
            },
            stateLink: 'streams.detail.overview.status(result.stateParams)',
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
            stateLink: 'datasets.detail.overview.status(result.stateParams)',
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
            stateLink: 'apps.detail.overview.programs(result.stateParams)',
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
            stateLink: getProgramLink(entityObj.id.type),
            programType: entityObj.id.type,
            type: entityObj.id.type,
            icon: (entityObj.id.type.toLowerCase() === 'flow'? 'icon-tigon' : 'icon-' + entityObj.id.type.toLowerCase())
          };
        default:
          return;
      }
    }

    function getProgramLink(programType) {
      switch (programType) {
        case 'Flow':
          return 'flows.detail(result.stateParams)';
        case 'Mapreduce':
          return 'mapreduce.detail(result.stateParams)';
        case 'Service':
          return 'services.detail(result.stateParams)';
        case 'Spark':
          return 'spark.detail(result.stateParams)';
        case 'Worker':
          return 'worker.detail(result.stateParams)';
        case 'Workflow':
          return 'workflows.detail(result.stateParams)';
      }
    }

  });
