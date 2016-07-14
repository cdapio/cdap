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

class HydratorPlusPlusStudioCtrl {
  // Holy cow. Much DI. Such angular.
  constructor(HydratorPlusPlusConfigActions, $stateParams, rConfig, $rootScope, $scope, DAGPlusPlusNodesActionsFactory, HydratorPlusPlusHydratorService, HydratorPlusPlusConsoleActions, rSelectedArtifact, rArtifacts, myLocalStorage, HydratorPlusPlusConfigStore, $window, HydratorPlusPlusConsoleTabService) {
    // This is required because before we fireup the actions related to the store, the store has to be initialized to register for any events.

    this.myLocalStorage = myLocalStorage;
    this.myLocalStorage
        .get('hydrator++-leftpanel-isExpanded')
        .then(isExpanded => this.isExpanded = (isExpanded === false ? false : true))
        .catch( () => this.isExpanded = true);
    // FIXME: This should essentially be moved to a scaffolding service that will do stuff for a state/view
    HydratorPlusPlusConsoleTabService.listen();
    $scope.$on('$destroy', () => {
      HydratorPlusPlusConsoleTabService.unsubscribe();
      HydratorPlusPlusConsoleActions.resetMessages();
      $window.onbeforeunload = null;
    });

    let getValidArtifact = () => {
      let isValidArtifact;
      if (rArtifacts.length) {
        isValidArtifact = rArtifacts.filter(r => r.name === rSelectedArtifact);
      }
      return isValidArtifact.length ? isValidArtifact[0]: rArtifacts[0];
    };
    let artifact = getValidArtifact();
    if (rConfig) {
      if (!rConfig.artifact){
        rConfig.artifact = artifact;
      }
      HydratorPlusPlusConfigActions.initializeConfigStore(rConfig);
      let configJson = rConfig;
      if (!rConfig.__ui__) {
        configJson = HydratorPlusPlusHydratorService.getNodesAndConnectionsFromConfig(rConfig, true);
        configJson['__ui__'] = {
          nodes: configJson.nodes.map( (node) => {
            node.properties = node.plugin.properties;
            node.label = node.plugin.label;
            return node;
          })
        };
        configJson.config = {
          connections : configJson.connections
        };
      }

      DAGPlusPlusNodesActionsFactory.createGraphFromConfig(configJson.__ui__.nodes, configJson.config.connections, configJson.config.comments);
    } else {
      let config = {};
      config.artifact = artifact;
      HydratorPlusPlusConfigActions.initializeConfigStore(config);
    }

    function customConfirm(message){
      var start = Date.now();
      var result = confirm(message);
      var timeDifference = Date.now() - start;
      /*
        FIXME: This can easily be prevented if we upgrade to angular ui router version > 0.2.16.

        This is just to confirm if the suppression is by the browser or
         a human interventation (too quick a reply). If the time difference is less then 50ms
         In the worst case a super human clicks on ok or cancel within 50ms and we show the confirm
         popup once again. Otherwise this loop is to check if its a browser native supression.
         If it is then we just return true.

         The reasoning behind this is the user has selected 'Prevent this page from showing any additional dialogs' while clicking OK in the popup and so any dirty state checks are inherently skipped based on that choice.
      */
      for(var i=0; i < 10 && !result && timeDifference < 50; i++){
        start = Date.now();
        result = confirm(message);
        timeDifference = Date.now() - start;
      }
      if(timeDifference < 50) {
        return true;
      }
      return result;
    }

    var confirmOnPageExit = function (e) {

      if (!HydratorPlusPlusConfigStore.getIsStateDirty()) { return; }
      // If we haven't been passed the event get the window.event
      e = e || $window.event;
      var message = 'You have unsaved changes.';
      // For IE6-8 and Firefox prior to version 4
      if (e) {
        e.returnValue = message;
      }
      // For Chrome, Safari, IE8+ and Opera 12+
      return message;
    };
    $window.onbeforeunload = confirmOnPageExit;

    $scope.$on('$stateChangeStart', function (event) {
      if (HydratorPlusPlusConfigStore.getIsStateDirty()) {
        var response = customConfirm('You have unsaved changes. Are you sure you want to exit this page?');
        if (!response) {
          event.preventDefault();
        }
      }
    });
  }
  toggleSidebar() {
    this.isExpanded = !this.isExpanded;
    this.myLocalStorage.set('hydrator++-leftpanel-isExpanded', this.isExpanded);
  }
}

HydratorPlusPlusStudioCtrl.$inject = ['HydratorPlusPlusConfigActions', '$stateParams', 'rConfig', '$rootScope', '$scope', 'DAGPlusPlusNodesActionsFactory', 'HydratorPlusPlusHydratorService', 'HydratorPlusPlusConsoleActions','rSelectedArtifact', 'rArtifacts', 'myLocalStorage', 'HydratorPlusPlusConfigStore', '$window', 'HydratorPlusPlusConsoleTabService'];

angular.module(PKG.name + '.feature.hydratorplusplus')
  .controller('HydratorPlusPlusStudioCtrl', HydratorPlusPlusStudioCtrl);
