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

class HydratorCreateStudioController {
  constructor(LeftPanelStore, LeftPanelActionsFactory, ConfigActionsFactory, $stateParams, rConfig, ConfigStore, $rootScope) {
    // This is required because before we fireup the actions related to the store, the store has to be initialized to register for any events.

    this.LeftPanelActionsFactory = LeftPanelActionsFactory;

    ConfigStore.setDefaults();
    if ($stateParams.type) {
      ConfigActionsFactory.setArtifact({
        version: $rootScope.cdapVersion,
        name: $stateParams.type,
        scope: 'SYSTEM'
      });
    }
    if (rConfig) {
      ConfigActionsFactory.setArtifact(rConfig.artifact);
      ConfigActionsFactory.setName(rConfig.name);
      ConfigActionsFactory.setDescription(rConfig.description);
    }

    this.isExpanded = LeftPanelStore.getState();
    LeftPanelStore.registerOnChangeListener( () => {
      this.isExpanded = LeftPanelStore.getState();
    });

  }

  toggleSidebar() {
    this.LeftPanelActionsFactory.togglePanel();
  }
}

HydratorCreateStudioController.$inject = ['LeftPanelStore', 'LeftPanelActionsFactory', 'ConfigActionsFactory', '$stateParams', 'rConfig', 'ConfigStore', '$rootScope'];
angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorCreateStudioController', HydratorCreateStudioController);
