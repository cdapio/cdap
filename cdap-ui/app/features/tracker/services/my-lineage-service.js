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

class myLineageService {

  /**
   *  Takes in the response from backend, and returns an object with list of
   *  nodes and connections.
   **/
  parseLineageResponse(response) {
    let connections = [];
    let uniqueNodes = {};

    /* SETTING NODES */

    angular.forEach(response.programs, (value, key) => {
      uniqueNodes[key] = {
        label: value.entityId.id.id,
        id: key,
        nodeType: 'PROGRAM',
        applicationId: value.entityId.id.application.applicationId,
        programId: value.entityId.id.id,
        programType: this.parseProgramType(value.entityId.id.type),
        runs: []
      };
    });

    angular.forEach(response.data, (value, key) => {
      let data = this.parseDataInfo(value);

      uniqueNodes[key] = {
        label: data.name,
        id: key,
        nodeType: 'DATA',
        entityId: data.name,
        entityType: data.type
      };
    });


    /* SETTING CONNECTIONS */
    angular.forEach(response.relations, (rel) => {
      let isUnknownOrBoth = rel.access === 'both' || rel.access === 'unknown';

      if (rel.access === 'read' || isUnknownOrBoth) {
        connections.push({
          source: rel.data,
          target: rel.program
        });
      }

      if (rel.access === 'write' || isUnknownOrBoth) {
        connections.push({
          source: rel.program,
          target: rel.data
        });
      }

      uniqueNodes[rel.program].runs = uniqueNodes[rel.program].runs.concat(rel.runs);
    });

    let graph = this.getGraphLayout(uniqueNodes, connections);
    this.mapNodesLocation(uniqueNodes, graph);

    return {
      connections: connections,
      nodes: uniqueNodes
    };
  }

  parseProgramType(programType) {
    let program = '';
    switch (programType) {
      case 'Flows':
        program = 'flows';
        break;
      case 'Mapreduce':
        program = 'mapreduce';
        break;
      case 'Spark':
        program = 'spark';
        break;
      case 'Worker':
        program = 'workers';
        break;
      case 'Workflow':
        program = 'workflows';
        break;
      case 'Service':
        program = 'services';
        break;
    }

    return program;
  }

  parseDataInfo(data) {
    let obj = {};
    if (data.entityId.type === 'datasetinstance') {
      obj = {
        name: data.entityId.id.instanceId,
        entityType: 'datasets'
      };
    } else {
      obj = {
        name: data.entityId.id.streamName,
        type: 'streams'
      };
    }

    return obj;
  }

  getGraphLayout(nodes, connections) {
    var graph = new dagre.graphlib.Graph();
    graph.setGraph({
      nodesep: 90,
      ranksep: 150,
      rankdir: 'LR',
      marginx: 0,
      marginy: 0
    });
    graph.setDefaultEdgeLabel(function() { return {}; });

    angular.forEach(nodes, (node) => {
      var id = node.id;
      graph.setNode(id, { label: node.label, width: 150, height: 100 });
    });

    angular.forEach(connections, (connection) => {
      graph.setEdge(connection.source, connection.target);
    });

    dagre.layout(graph);

    return graph;
  }

  mapNodesLocation(nodes, graph) {
    angular.forEach(graph._nodes, (value, key) => {
      nodes[key]._uiLocation = {
        top: value.y + 'px',
        left: value.x + 'px'
      };
    });
  }
}

myLineageService.$inject = [];

angular.module(PKG.name + '.feature.tracker')
  .service('myLineageService', myLineageService);
