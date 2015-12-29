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

/*
  This solely exists because of the circular dependency complain from angular.
  Not sure why a error factory should not be dependent on a store and vice-versa at the same time (store<->errorfactory)
  These utility functions should have been in HydratorErrorFactory.
  Its not there because of this reason,
    [$injector:cdep] Circular dependency found: ConfigStore <- HydratorErrorFactory <- ConfigStore <- LeftPanelStore

  The only valid (sortof) justification I have as of now for creating this factory is all these functions are sort of pure functions
  in a way you give it an input it gives back an output. No store dependency.
*/

let countRequiredFields = (node) => {
  var requiredFieldCount = 0;
  if (angular.isObject(node._backendProperties) && Object.keys(node._backendProperties).length) {

    angular.forEach(node._backendProperties, function (value, key) {
      if (value.required) {
        if (!node.plugin.properties || !node.plugin.properties[key]) {
          requiredFieldCount++;
        }
      }
    });
  }
  return requiredFieldCount;
};

let updatePluginsWithRequiredFieldsCount = (GLOBALS, nodes = [])=> {
  if (!nodes.length) {
    return;
  }
  let isRuleValid = true;
  nodes.forEach( node => {
    node.requiredFieldCount = countRequiredFields(node);
    if (node.requiredFieldCount > 0) {
      node.error = {
        message: GLOBALS.en.hydrator.studio.genericMissingRequiredFieldsError
      };
      isRuleValid = false;
    } else {
      node.error = false;
    }
  });
};

let checkAndUpdateUniqueNodeNames = (nodes = []) => {
  if (!nodes.length){
    return;
  }

  let nodesIdMap = {};
  let isRuleValid = true;
  angular.forEach(nodes, function (node) {
    if (!nodesIdMap[node.plugin.label]) {
      nodesIdMap[node.plugin.label] = [];
    }
    nodesIdMap[node.plugin.label].push(node);
  });
  angular.forEach(nodesIdMap, function(nodeArray) {
    if(nodeArray.length > 1) {
      isRuleValid = false;
      nodeArray.forEach( function(n) {
        n.error = n.error || {};
        n.error.message = `Node with name '${n.plugin.label}' already exists`;
        n.requiredFieldCount = '!';
      });
    }
  });
  return isRuleValid;
};

let NonStorePipelineErrorFactory = (GLOBALS) => {
  return {
    checkAndUpdateUniqueNodeNames: checkAndUpdateUniqueNodeNames,
    updatePluginsWithRequiredFieldsCount: updatePluginsWithRequiredFieldsCount.bind(null, GLOBALS),
    countRequiredFields: countRequiredFields
  };
};

NonStorePipelineErrorFactory.$inject = ['GLOBALS'];

angular.module(`${PKG.name}.feature.hydrator`)
  .factory('NonStorePipelineErrorFactory', NonStorePipelineErrorFactory);
