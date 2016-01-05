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

/*
  DAG level validation functions get the input, iterate through the data, validate each data and then call the callback for each data.
  The validation function doesn't mutate the data. The callback that gets called can decide whether to change it or just use the error message.

  In this case DAG level validations generally are based on list of nodes hence this pattern. This is not perfect yet but
  for a list of "data" and possibly more than one type of error calling the callback for each "data" works now. We need to evolve the error handling
  in hydrator.

*/

let countUnFilledRequiredFields = (node) => {
  var requiredFieldCount = 0;
  if (angular.isObject(node._backendProperties) && Object.keys(node._backendProperties).length) {
    angular.forEach(node._backendProperties, (value, key) => {
      if (value.required) {
        if (!node.plugin.properties || !node.plugin.properties[key]) {
          requiredFieldCount++;
        }
      }
    });
  }
  return requiredFieldCount;
};

let isRequiredFieldsFilled = (myHelpers, nodes, cb) => {
  if (!myHelpers.objectQuery(nodes, 'length')) {
    return;
  }
  nodes.forEach( node => {
    let unFilledRequiredFieldsCount = countUnFilledRequiredFields(node);
    let error = 'MISSING-REQUIRED-FIELDS';
    if (unFilledRequiredFieldsCount > 0) {
      cb(error, node, unFilledRequiredFieldsCount);
    } else {
      cb(false, node, unFilledRequiredFieldsCount);
    }
  });
};

let isUniqueNodeNames = (myHelpers, nodes, cb) => {
  let isRuleValid = true;
  if (!myHelpers.objectQuery(nodes, 'length')) {
    return isRuleValid;
  }
  let nodesIdMap = {};
  angular.forEach(nodes, function (node) {
    if (!nodesIdMap[node.plugin.label]) {
      nodesIdMap[node.plugin.label] = [];
    }
    nodesIdMap[node.plugin.label].push(node);
  });
  angular.forEach(nodesIdMap, function(nodeArray) {
    if(nodeArray.length > 1) {
      nodeArray.forEach( function(n) {
        let error = 'DUPLICATE-NAME';
        cb(error, n);
      });
    } else {
      cb(false, nodeArray[0]);
    }
  });
  return isRuleValid;
};

let isNodeNameUnique = (myHelpers, nodeName, nodes, cb) => {
  if (!myHelpers.objectQuery(nodes, 'length') || !nodeName.length) {
    cb(false);
    return;
  }
  let error;
  let filteredNames = nodes.filter( node => {
    return node.plugin.label === nodeName;
  });
  if (filteredNames.length > 1) {
    error = 'DUPLICATE-NAME';
    cb(error);
    return;
  }
  cb(false);
};


let hasValidName = (name, cb) => {
  let error;
  let pattern = /^[\w]+$/;
  if (!name) {
    error = 'MISSING-NAME';
    cb(error);
    return;
  }
  if (!pattern.test(name)) {
    error = 'INVALID-NAME';
    cb(error);
    return;
  }
  cb(false);
};

let hasOnlyOneSource = (myHelpers, GLOBALS, nodes, cb) => {
  let error;
  let countSource;
  if (!myHelpers.objectQuery(nodes, 'length')) {
    cb(false);
  }
  nodes.forEach( node => {
    if (GLOBALS.pluginConvert[node.type] === 'source') {
      countSource++;
    }
  });
  if (countSource === 0) {
    error = 'NO-SOURCE-FOUND';
    cb(error);
    return;
  }
  if (countSource > 1) {
    error = 'MORE-THAN-ONE-SOURCE-FOUND';
    cb(error);
    return;
  }
  cb(false);
};

let hasAtLeastOneSink = (myHelpers, GLOBALS, nodes, cb) => {
  let error;
  let countSink;
  if (!myHelpers.objectQuery(nodes, 'length')) {
    cb(false);
  }
  nodes.forEach( node => {
    if (GLOBALS.pluginConvert[node.type] === 'sink') {
      countSink++;
    }
  });
  if (countSink === 0) {
    error = 'NO-SINK-FOUND';
    cb(error);
    return;
  }
  cb(false);
};

let NonStorePipelineErrorFactory = (GLOBALS, myHelpers) => {
  // If we had used SystemJs or requirejs this could have been avoided.
  return {
    isUniqueNodeNames: isUniqueNodeNames.bind(null, myHelpers),
    isRequiredFieldsFilled: isRequiredFieldsFilled.bind(null, myHelpers),
    countUnFilledRequiredFields,
    hasValidName,
    hasOnlyOneSource: hasOnlyOneSource.bind(null, myHelpers, GLOBALS),
    hasAtLeastOneSink: hasAtLeastOneSink.bind(null, myHelpers, GLOBALS),
    isNodeNameUnique: isNodeNameUnique.bind(null, myHelpers)
  };
};

NonStorePipelineErrorFactory.$inject = ['GLOBALS', 'myHelpers'];

angular.module(`${PKG.name}.feature.hydrator`)
  .factory('NonStorePipelineErrorFactory', NonStorePipelineErrorFactory);
