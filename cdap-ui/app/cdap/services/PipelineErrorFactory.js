/*
 * Copyright © 2017 Cask Data, Inc.
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

import {objectQuery} from 'services/helpers';
import GLOBALS from 'services/global-constants';

let countUnFilledRequiredFields = (node) => {
  var requiredFieldCount = 0;
  if (typeof node._backendProperties === 'object' && Object.keys(node._backendProperties).length) {
    for (let key in node._backendProperties) {
      if (node._backendProperties.hasOwnProperty(key) && node._backendProperties[key].required) {
        if (!node.plugin.properties || !node.plugin.properties[key]) {
          requiredFieldCount++;
        }
      }
    }
  }
  return requiredFieldCount;
};

let isRequiredFieldsFilled = (nodes, cb) => {
  if (!objectQuery(nodes, 'length')) {
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

let isUniqueNodeNames = (nodes, cb) => {
  let isRuleValid = true;
  if (!objectQuery(nodes, 'length')) {
    return isRuleValid;
  }
  let nodesIdMap = {};
  nodes.forEach(node => {
    let nodeName = node.plugin.label || node.name;
    if (!nodesIdMap[nodeName]) {
      nodesIdMap[nodeName] = [];
    }
    nodesIdMap[nodeName].push(node);
  });
  for (let nodeName in nodesIdMap) {
    if (nodesIdMap.hasOwnProperty(nodeName)) {
      let nodeArray = nodesIdMap[nodeName];
      if (nodeArray.length > 1) {
        nodeArray.forEach( function(n) {
          let error = 'DUPLICATE-NAME';
          cb(error, n);
        });
      } else {
        cb(false, nodeArray[0]);
      }
    }
  }
  return isRuleValid;
};

let hasAtleastOneSource = (nodes, cb) => {
  let error;
  let countSource = 0;
  if (!objectQuery(nodes, 'length')) {
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

let isNodeNameUnique = (nodeName, nodes, cb) => {
  if (!objectQuery(nodes, 'length') || !nodeName.length) {
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

let hasAtLeastOneSink = (nodes, cb) => {
  let error;
  let countSink = 0;
  if (!objectQuery(nodes, 'length')) {
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

let allNodesConnected = (nodes, connections, cb) => {
  // check if there is only 1 node and node is of type action
  if (connections.length === 0 && nodes.length === 1) {
    if (GLOBALS.pluginConvert[nodes[0].type] === 'action') {
      return;
    }
  }

  let inputConnection = {};
  let outputConnection = {};

  connections.forEach(connection => {
    inputConnection[connection.to] = connection.from;
    outputConnection[connection.from] = connection.to;
  });

  nodes.forEach(node => {
    let type = node.type || node.plugin.type;
    switch (GLOBALS.pluginConvert[type]) {
      case 'source':
        if (!outputConnection[node.name]) {
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

let connectionIsValid = (fromNode, toNode, cb) => {

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

let allConnectionsValid = (nodes, connections, cb) => {
  let nodesMap = {};
  nodes.forEach(node => {
    nodesMap[node.name] = node;
  });

  connections.forEach(conn => {
    let from = nodesMap[conn.from],
        to = nodesMap[conn.to];

    connectionIsValid(from, to, cb);
  });
};

let hasValidArtifact = (importConfig) => {
  return importConfig.artifact && importConfig.artifact.name.length && importConfig.artifact.version.length && importConfig.artifact.scope.length;
};
let hasValidConfig = (importConfig) => {
  return importConfig.config;
};
let hasValidSchedule = (importConfig) => {
  console.log(GLOBALS);
  let isBatchPipeline = GLOBALS.etlBatchPipelines.indexOf(importConfig.artifact.name) !== -1;
  return !isBatchPipeline? true: importConfig.config.schedule;
};
let hasValidInstance = (importConfig) => {
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
  } else if (config.stages) {
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
let validateImportJSON = (configString) => {
  let config;
  try {
    config = JSON.parse(configString);
  } catch (e) {
    let messagePath = errorPath.concat(['INVALID-SYNTAX']);
    return objectQuery.apply(null, [GLOBALS].concat(messagePath));
  }

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
  for (i = 0; i < validations.length; i++) {
    let currValidation = validations[i];
    if (!currValidation.fn.call(null, config)) {
      return objectQuery.apply(null, [GLOBALS].concat(currValidation.messagePath));
    }
  }
  return false;
};

export {
  isUniqueNodeNames,
  isRequiredFieldsFilled,
  countUnFilledRequiredFields,
  hasValidName,
  hasValidResources,
  hasValidDriverResources,
  hasValidClientResources,
  hasAtleastOneSource,
  hasAtLeastOneSink,
  isNodeNameUnique,
  allNodesConnected,
  allConnectionsValid,
  connectionIsValid,
  validateImportJSON
};
