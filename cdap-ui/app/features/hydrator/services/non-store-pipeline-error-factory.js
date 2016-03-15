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
  if (!node.plugin.label) {
    requiredFieldCount++;
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
    // Check for multiple nodes with empty labels
    // Multiple empty labels shouldn't be considered duplicates
    if (!node.plugin.label) {
      return;
    }
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
  let countSource = 0;
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
  let countSink = 0;
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

let allNodesConnected = (GLOBALS, nodes, connections, cb) => {
  let inputConnection = {};
  let outputConnection = {};

  let errors = [];
  angular.forEach(connections, (connection) => {
    inputConnection[connection.to] = connection.from;
    outputConnection[connection.from] = connection.to;
  });

  angular.forEach(nodes, (node) => {
    switch (GLOBALS.pluginConvert[node.type]) {
      case 'source':
        if (!outputConnection[node.name]){
          errors.push(node.plugin.label);
          cb(node);
        }
        break;
      case 'transform':
        if (!inputConnection[node.name] || !outputConnection[node.name]) {
          cb(node);
        }
        break;
      case 'sink':
        if (!inputConnection[node.name]) {
          cb(node);
        }
        break;
    }
  });
};

let hasValidArtifact = (importConfig) => {
  return importConfig.artifact && importConfig.artifact.name.length && importConfig.artifact.version.length && importConfig.artifact.scope.length;
};
let hasValidSource = (importConfig) => {
  return importConfig.config.source;
};
let hasValidSinks = (importConfig) => {
  return importConfig.config.sinks && importConfig.config.sinks.length;
};
let hasValidConfig = (importConfig) => {
  return importConfig.config;
};
let hasValidSchedule = (importConfig, GLOBALS) => {
  let isBatchPipeline = importConfig.artifact.name === GLOBALS.etlBatch;
  return !isBatchPipeline? true: importConfig.config.schedule;
};
let hasValidInstance = (importConfig, GLOBALS) => {
  let isRealtimePipeline = importConfig.artifact.name === GLOBALS.etlRealtime;
  return !isRealtimePipeline? true: importConfig.config.instance;
};
let hasValidNodesConnections = (importConfig) => {
  if (!importConfig.config.connections) {
    return true;
  }
  let config = importConfig.config;
  let isValid = true;
  let nodesMap = {};
  [config.source].concat(config.sinks)
    .concat( (config.transforms || []) )
    .forEach( node => nodesMap[node.name] = node);
  config.connections.forEach( conn => {
    isValid = isValid && (nodesMap[conn.from] && nodesMap[conn.to]);
  });
  return isValid;
};

let validateImportJSON = (myHelpers, GLOBALS, config) => {
  let errorPath = ['en', 'hydrator', 'studio', 'error', 'IMPORT-JSON'];
  let validations = [
    { fn: hasValidArtifact, messagePath: errorPath.concat(['INVALID-ARTIFACT']) },
    { fn: hasValidConfig, messagePath: errorPath.concat(['INVALID-CONFIG']) },
    { fn: hasValidSchedule, messagePath: errorPath.concat(['INVALID-SCHEDULE']) },
    { fn: hasValidInstance, messagePath: errorPath.concat(['INVALID-INSTANCE']) },
    { fn: hasValidSource, messagePath: errorPath.concat(['INVALID-SOURCE']) },
    { fn: hasValidSinks, messagePath: errorPath.concat(['INVALID-SINKS']) },
    { fn: hasValidNodesConnections, messagePath: errorPath.concat(['INVALID-NODES-CONNECTIONS']) }
  ];
  let i;
  for(i=0; i<validations.length; i++) {
    let currValidation = validations[i];
    if (!currValidation.fn.call(null, config, GLOBALS)) {
      return myHelpers.objectQuery.apply(null, [GLOBALS].concat(currValidation.messagePath));
    }
  }
  return false;
};

let NonStorePipelineErrorFactory = (GLOBALS, myHelpers) => {
  // If we had used SystemJs or requirejs this could have been avoided.
  return {
    isUniqueNodeNames: isUniqueNodeNames.bind(null, myHelpers),
    isRequiredFieldsFilled: isRequiredFieldsFilled.bind(null, myHelpers),
    countUnFilledRequiredFields: countUnFilledRequiredFields,
    hasValidName: hasValidName,
    hasOnlyOneSource: hasOnlyOneSource.bind(null, myHelpers, GLOBALS),
    hasAtLeastOneSink: hasAtLeastOneSink.bind(null, myHelpers, GLOBALS),
    isNodeNameUnique: isNodeNameUnique.bind(null, myHelpers),
    allNodesConnected: allNodesConnected.bind(null, GLOBALS),
    validateImportJSON: validateImportJSON.bind(null, myHelpers, GLOBALS)
  };
};

NonStorePipelineErrorFactory.$inject = ['GLOBALS', 'myHelpers'];

angular.module(`${PKG.name}.feature.hydrator`)
  .factory('NonStorePipelineErrorFactory', NonStorePipelineErrorFactory);
