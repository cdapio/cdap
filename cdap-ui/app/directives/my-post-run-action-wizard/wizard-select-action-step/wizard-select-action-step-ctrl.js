/*
 * Copyright © 2016 Cask Data, Inc.
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

class WizardSelectActionStepCtrl {
  constructor($scope, $state, myPipelineApi, myHelpers, GLOBALS) {
    let artifact = this.store.getArtifact();
    this.postActionsList = [];
    this.$scope = $scope;

    this.onItemClicked = function(event, action) {
      this.chooseAction(action);
    };
    this.loadingPlugins = true;

    let params = {
      namespace: $state.params.namespace,
      pipelineType: artifact.name,
      version: artifact.version,
      extensionType: 'postaction'
    };
    myPipelineApi.fetchPlugins(params)
      .$promise
      .then(
        (res) => {
          let filteredPlugins = this.filterPlugins(res);

          this.postActionsList = Object.keys(filteredPlugins).map( postaction => {
            // Coverting the name to lowercase before lookup as we can maintain a case insensitive map in case backend wants to change from camelcase or to any other case.
            return Object.assign({}, filteredPlugins[postaction], {
              template: '/old_assets/features/hydratorplusplus/templates/create/popovers/leftpanel-plugin-popover.html',
              label: myHelpers.objectQuery(
                GLOBALS.pluginTypes, 'post-run-actions', filteredPlugins[postaction].name.toLowerCase()
              ) || filteredPlugins[postaction].name,
              description: filteredPlugins[postaction].description || ''
            });
          });
          this.loadingPlugins = false;
        },
        (err) => {
          this.loadingPlugins = false;
          console.log('ERROR: ', err);
        }
      );
  }
  chooseAction(action) {
    var fn = this.onActionSelect();
    if ('undefined' !== typeof fn) {
      fn.call(null, action);
    }
  }
  filterPlugins(results) {
    let pluginsMap = {};
    angular.forEach(results, (plugin) => {
      if (!pluginsMap[plugin.name]) {
        pluginsMap[plugin.name] = Object.assign({}, plugin, {
          defaultArtifact: plugin.artifact,
          allArtifacts: []
        });
      }
      pluginsMap[plugin.name].allArtifacts.push(plugin);
    });
    return pluginsMap;
  }
}

WizardSelectActionStepCtrl.$inject = ['$scope', '$state', 'myPipelineApi', 'myHelpers', 'GLOBALS'];
angular.module(PKG.name + '.commons')
  .controller('WizardSelectActionStepCtrl', WizardSelectActionStepCtrl);
