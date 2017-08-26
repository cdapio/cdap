/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
    [$injector:cdep] Circular dependency found: ConfigStore <- HydratorErrorFactory <- ConfigStore <- HydratoryPlusPlusLeftPanelStore

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
    let nodeName = node.plugin.label || node.name;
    if (!nodesIdMap[nodeName]) {
      nodesIdMap[nodeName] = [];
    }
    nodesIdMap[nodeName].push(node);
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
let hasAtleastOneSource = (myHelpers, GLOBALS, nodes, cb) => {
  let error;
  let countSource = 0;
  if (!myHelpers.objectQuery(nodes, 'length')) {
    cb(false);
  }
  nodes.forEach( node => {
    let type = node.type || node.plugin.type;
    if (GLOBALS.pluginConvert[type] === 'source') {
      countSource++;
    }
  });
  if (countSource === 0) {
    error = 'NO-SOURCE-FOUND';
    cb(error);
    return;
   }
   cb(false);
 };
let isNodeNameUnique = (myHelpers, nodeName, nodes, cb) => {
  if (!myHelpers.objectQuery(nodes, 'length') || !nodeName.length) {
    cb(false);
    return;
  }
  let error;
  let filteredNames = nodes.filter( node => {
    let name = node.plugin.label || node.name;
    return name === nodeName;
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
  let pattern = /^[\w-]+$/;
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

let hasAtLeastOneSink = (myHelpers, GLOBALS, nodes, cb) => {
  let error;
  let countSink = 0;
  if (!myHelpers.objectQuery(nodes, 'length')) {
    cb(false);
  }
  nodes.forEach( node => {
    let type = node.type || node.plugin.type;
    if (GLOBALS.pluginConvert[type] === 'sink') {
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
  // check if there is only 1 node and node is of type action
  if (connections.length === 0 && nodes.length === 1) {
    if (GLOBALS.pluginConvert[nodes[0].type] === 'action') {
      return;
    }
  }

  let inputConnection = {};
  let outputConnection = {};

  angular.forEach(connections, (connection) => {
    inputConnection[connection.to] = connection.from;
    outputConnection[connection.from] = connection.to;
  });

  angular.forEach(nodes, (node) => {
    let type = node.type || node.plugin.type;
    switch (GLOBALS.pluginConvert[type]) {
      case 'source':
        if (!outputConnection[node.name]){
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
      case 'action':
        if (!inputConnection[node.name] && !outputConnection[node.name]) {
          cb(node);
        }
        break;
    }
  });
};

let connectionIsValid = (GLOBALS, fromNode, toNode, cb) => {

  /**
   * Rules:
   *    1. Source & Transform can only connect to Transform, Sink, or Condition
   *    2. Sink can only connect to Action, or Condition
   *    3. Action can only connect to Action, Source or Condition
   *    4. Condition can connect to anything
   **/

  let fromType = GLOBALS.pluginConvert[fromNode.type],
      toType = GLOBALS.pluginConvert[toNode.type];

  switch (fromType) {
    case 'source':
    case 'transform':
      if (!(toType === 'transform' || toType === 'sink' || toType === 'condition')) {
        cb(fromNode.plugin.label + ' → ' + toNode.plugin.label);
      }
      break;
    case 'sink':
      if (toType !== 'action' && toType !== 'condition') {
        cb(fromNode.plugin.label + ' → ' + toNode.plugin.label);
      }
      break;
    case 'action':
      if (!(toType === 'action' || toType === 'source' || toType === 'condition')) {
        cb(fromNode.plugin.label + ' → ' + toNode.plugin.label);
      }
      break;
  }
};

let allConnectionsValid = (GLOBALS, nodes, connections, cb) => {
  let nodesMap = {};
  angular.forEach(nodes, (node) => {
    nodesMap[node.name] = node;
  });

  angular.forEach(connections, (conn) => {
    let from = nodesMap[conn.from],
        to = nodesMap[conn.to];

    connectionIsValid(GLOBALS, from, to, cb);
  });
};

let hasValidArtifact = (importConfig) => {
  return importConfig.artifact && importConfig.artifact.name.length && importConfig.artifact.version.length && importConfig.artifact.scope.length;
};
let hasValidConfig = (importConfig) => {
  return importConfig.config;
};
let hasValidSchedule = (importConfig, GLOBALS) => {
  let isBatchPipeline = GLOBALS.etlBatchPipelines.indexOf(importConfig.artifact.name) !== -1;
  return !isBatchPipeline? true: importConfig.config.schedule;
};
let hasValidInstance = (importConfig, GLOBALS) => {
  let isRealtimePipeline = importConfig.artifact.name === GLOBALS.etlRealtime;
  return !isRealtimePipeline? true: importConfig.config.instances;
};
let hasValidConnections = (importConfig) => {
  return importConfig.connections && !importConfig.config.connections? false: true;
};
let hasStages = (importConfig) => {
  return importConfig.config.stages;
};
let hasValidStages = (importConfig) => {
  return !importConfig.stages;
};
let hasValidNodesConnections = (importConfig) => {
  if (!importConfig.config.connections) {
    return true;
  }
  let config = importConfig.config;
  let isValid = true;
  let nodesMap = {};
  let stages;
  if (importConfig.__ui__) {
    stages = importConfig.__ui__.nodes;
  } else if (config.stages){
    stages = config.stages;
  } else {
    stages = [];
  }
  stages.forEach( node => nodesMap[node.name] = node);
  config.connections.forEach( conn => {
    isValid = isValid && (nodesMap[conn.from] && nodesMap[conn.to]);
  });
  return isValid;
};
let isPipelineResourcesPositive = (resources) => {
  let isPositive = (num) => num > 0;
  return (isPositive(resources.virtualCores) && isPositive(resources.memoryMB));
};
let isInvalidResource = (resources) => {
  return (
    !resources ||
    !resources.virtualCores ||
    !resources.memoryMB
  );
};
let hasValidResources = (config, cb) => {
  let {resources} = config;
  if (isInvalidResource(resources)) {
    cb('MISSING-RESOURCES');
  } else if (!isPipelineResourcesPositive(resources)) {
    cb('INVALID-RESOURCES');
  }
};
let hasValidDriverResources = (config, cb) => {
  let {driverResources} = config;
  if (isInvalidResource(driverResources)) {
    cb('MISSING-DRIVERRESOURCES');
  } else if (!isPipelineResourcesPositive(driverResources)) {
    cb('INVALID-DRIVERRESOURCES');
  }
};
let hasValidClientResources = (config, cb) => {
  let {clientResources} = config;
  if (isInvalidResource(clientResources)) {
    cb('MISSING-CLIENTRESOURCES');
  } else if (!isPipelineResourcesPositive(clientResources)) {
    cb('INVALID-CLIENTRESOURCES');
  }
};
let validateImportJSON = (myHelpers, GLOBALS, config) => {
  let errorPath = ['en', 'hydrator', 'studio', 'error', 'IMPORT-JSON'];
  let validations = [
    { fn: hasValidArtifact, messagePath: errorPath.concat(['INVALID-ARTIFACT']) },
    { fn: hasValidConfig, messagePath: errorPath.concat(['INVALID-CONFIG']) },
    { fn: hasValidSchedule, messagePath: errorPath.concat(['INVALID-SCHEDULE']) },
    { fn: hasValidInstance, messagePath: errorPath.concat(['INVALID-INSTANCE']) },
    { fn: hasValidNodesConnections, messagePath: errorPath.concat(['INVALID-NODES-CONNECTIONS']) },
    { fn: hasValidConnections, messagePath: errorPath.concat(['INVALID-CONNECTIONS']) },
    { fn: hasStages, messagePath: errorPath.concat(['NO-STAGES']) },
    { fn: hasValidStages, messagePath: errorPath.concat(['INVALID-STAGES']) }
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
    hasValidResources: hasValidResources,
    hasValidDriverResources: hasValidDriverResources,
    hasValidClientResources: hasValidClientResources,
    hasAtleastOneSource: hasAtleastOneSource.bind(null, myHelpers, GLOBALS),
    hasAtLeastOneSink: hasAtLeastOneSink.bind(null, myHelpers, GLOBALS),
    isNodeNameUnique: isNodeNameUnique.bind(null, myHelpers),
    allNodesConnected: allNodesConnected.bind(null, GLOBALS),
    allConnectionsValid: allConnectionsValid.bind(null, GLOBALS),
    connectionIsValid: connectionIsValid.bind(null, GLOBALS),
    validateImportJSON: validateImportJSON.bind(null, myHelpers, GLOBALS)
  };
};

NonStorePipelineErrorFactory.$inject = ['GLOBALS', 'myHelpers'];

angular.module(`${PKG.name}.feature.hydrator`)
  .factory('NonStorePipelineErrorFactory', NonStorePipelineErrorFactory);
