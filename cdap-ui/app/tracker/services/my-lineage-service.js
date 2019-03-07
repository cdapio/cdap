/*
 * Copyright © 2016-2018 Cask Data, Inc.
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
  constructor($state, myTrackerApi) {
    this.$state = $state;
    this.myTrackerApi = myTrackerApi;
  }

  /**
   *  Takes in the response from backend, and returns an object with list of
   *  nodes and connections.
   **/
  parseLineageResponse(response, params) {
    let currentActiveNode = [
      'dataset',
      params.namespace,
      params.entityId
    ].join('.');

    let connections = [];
    let uniqueNodes = {};
    let nodes = [];

    /* SETTING NODES */
    angular.forEach(response.programs, (value, key) => {
      let entityId = value.entityId.program;

      let nodeObj = {
        label: entityId,
        id: key,
        nodeType: 'program',
        applicationId: value.entityId.application,
        entityId: entityId,
        entityType: this.parseProgramType(value.entityId.type),
        displayType: value.entityId.type,
        icon: this.getProgramIcon(value.entityId.type),
        runs: []
      };

      uniqueNodes[key] = nodeObj;
    });

    angular.forEach(response.data, (value, key) => {
      let data = this.parseDataInfo(value);

      let nodeObj = {
        label: data.name,
        id: key,
        nodeType: 'data',
        entityId: data.name,
        entityType: data.type,
        displayType: data.displayType,
        icon: data.icon
      };

      uniqueNodes[key] = nodeObj;

      if (data.type === 'datasets') {
        let params = {
          namespace: this.$state.params.namespace,
          entityType: data.type,
          entityId: data.name
        };
        this.myTrackerApi.getDatasetSystemProperties(params)
          .$promise
          .then( (res) => {
            let parsedType = res.type.split('.');
            nodeObj.displayType = parsedType[parsedType.length - 1];
          });
      }

    });


    /* SETTING CONNECTIONS */
    angular.forEach(response.relations, (rel) => {
      let isUnknownOrBoth = rel.access === 'both' || rel.access === 'unknown';

      if (rel.access === 'read' || isUnknownOrBoth) {
        let dataId = rel.data === currentActiveNode ? rel.data : rel.data + '-read';
        let programId = rel.data === currentActiveNode ? rel.program + '-read' : rel.program + '-write';
        connections.push({
          source: dataId,
          target: programId,
          type: 'read'
        });

        nodes.push({
          dataId: dataId,
          uniqueNodeId: rel.data,
          isLeftEdge: rel.data !== currentActiveNode
        });
        nodes.push({
          dataId: programId,
          uniqueNodeId: rel.program
        });
      }

      if (rel.access === 'write' || isUnknownOrBoth) {
        let dataId = rel.data === currentActiveNode ? rel.data : rel.data + '-write';
        let programId = rel.data === currentActiveNode ? rel.program + '-write' : rel.program + '-read';
        connections.push({
          source: programId,
          target: dataId,
          type: 'write'
        });

        nodes.push({
          dataId: dataId,
          uniqueNodeId: rel.data,
          isRightEdge: rel.data !== currentActiveNode
        });
        nodes.push({
          dataId: programId,
          uniqueNodeId: rel.program
        });
      }

      uniqueNodes[rel.program].runs = uniqueNodes[rel.program].runs.concat(rel.runs);
      uniqueNodes[rel.program].runs = _.uniq(uniqueNodes[rel.program].runs);
    });

    nodes = _.uniq(nodes, (n) => { return n.dataId; });
    let graph = this.getGraphLayout(nodes, connections);
    this.mapNodesLocation(nodes, graph);

    return {
      connections: connections,
      nodes: nodes,
      uniqueNodes: uniqueNodes,
      graph: graph
    };
  }

  secondLineageParser(response, params) {
    let currentActiveNode = [
      'dataset',
      params.namespace,
      params.entityId
    ].join('.');

    let connections = [];
    let uniqueNodes = {};
    let nodes = [];

    /* SETTING NODES */
    angular.forEach(response.programs, (value, key) => {
      let entityId = value.entityId.program;

      let nodeObj = {
        label: entityId,
        id: key,
        nodeType: 'program',
        applicationId: value.entityId.application,
        entityId: entityId,
        entityType: this.parseProgramType(value.entityId.type),
        displayType: value.entityId.type,
        icon: this.getProgramIcon(value.entityId.type),
        runs: []
      };

      uniqueNodes[key] = nodeObj;
    });

    angular.forEach(response.data, (value, key) => {
      let data = this.parseDataInfo(value);

      let nodeObj = {
        label: data.name,
        id: key,
        nodeType: 'data',
        entityId: data.name,
        entityType: data.type,
        displayType: data.displayType,
        icon: data.icon
      };

      uniqueNodes[key] = nodeObj;

      if (data.type === 'datasets') {
        let params = {
          namespace: this.$state.params.namespace,
          entityType: data.type,
          entityId: data.name
        };
        this.myTrackerApi.getDatasetProperties(params)
          .$promise
          .then( (res) => {
            const type = res.find((property) => {
              return property.scope === 'SYSTEM' && property.name === 'type';
            });

            if (type) {
              let parsedType = type.value.split('.');
              nodeObj.displayType = parsedType[parsedType.length - 1];
            }
          });
      }
    });

    /* SETTING CONNECTIONS */
    angular.forEach(response.relations, (rel) => {
      let isUnknownOrBoth = rel.accesses.length > 1;
      if (!isUnknownOrBoth && rel.accesses[0] === 'read') {
        let dataId = rel.data;
        let programId = rel.program;
        connections.push({
          source: dataId,
          target: programId,
          type: 'read'
        });

        nodes.push({
          dataId: dataId,
          uniqueNodeId: rel.data,
          isLeftEdge: rel.data !== currentActiveNode
        });
        nodes.push({
          dataId: programId,
          uniqueNodeId: rel.program
        });
      }

      if (rel.accesses[0] !== 'read' || isUnknownOrBoth) {
        let dataId = rel.data;
        let programId = rel.program;
        connections.push({
          source: programId,
          target: dataId,
          type: 'write'
        });

        nodes.push({
          dataId: dataId,
          uniqueNodeId: rel.data,
          isRightEdge: rel.data !== currentActiveNode
        });
        nodes.push({
          dataId: programId,
          uniqueNodeId: rel.program
        });
      }

      uniqueNodes[rel.program].runs = uniqueNodes[rel.program].runs.concat(rel.runs);
      uniqueNodes[rel.program].runs = _.uniq(uniqueNodes[rel.program].runs);
    });

    nodes = _.uniq(nodes, (n) => { return n.dataId; });
    let graph = this.getGraphLayout(nodes, connections);
    this.mapNodesLocation(nodes, graph);

    return {
      connections: connections,
      nodes: nodes,
      uniqueNodes: uniqueNodes,
      graph: graph
    };

  }

  parseProgramType(programType) {
    switch (programType) {
      case 'Flow':
      case 'flow':
      case 'Flows':
      case 'flows':
        return 'flows';
      case 'Mapreduce':
      case 'mapreduce':
        return 'mapreduce';
      case 'Spark':
      case 'spark':
        return 'spark';
      case 'Worker':
      case 'worker':
      case 'Workers':
      case 'workers':
        return'workers';
      case 'Workflow':
      case 'workflow':
      case 'Workflows':
      case 'workflows':
        return 'workflows';
      case 'Service':
      case 'service':
      case 'Services':
      case 'service':
        return 'services';
    }
  }

  parseDataInfo(data) {
    if (data.entityId.entity === 'DATASET') {
      return {
        name: data.entityId.dataset,
        type: 'datasets',
        icon: 'icon-datasets',
        displayType: 'Dataset'
      };
    }
  }

  getProgramIcon(programType) {
    let iconMap = {
      'Flow': 'icon-tigon',
      'Mapreduce': 'icon-mapreduce',
      'Spark': 'icon-spark',
      'Worker': 'icon-worker',
      'Workflow': 'icon-workflow',
      'Service': 'icon-service'
    };

    return iconMap[programType];
  }

  getGraphLayout(nodes, connections) {
    var graph = new dagre.graphlib.Graph();
    graph.setGraph({
      nodesep: 50,
      ranksep: 90,
      rankdir: 'LR',
      marginx: 100,
      marginy: 50
    });
    graph.setDefaultEdgeLabel(function() { return {}; });

    angular.forEach(nodes, (node) => {
      var id = node.dataId;
      graph.setNode(id, { width: 180, height: 60 });
    });

    angular.forEach(connections, (connection) => {
      graph.setEdge(connection.source, connection.target);
    });

    dagre.layout(graph);

    return graph;
  }

  mapNodesLocation(nodes, graph) {
    angular.forEach(nodes, (node) => {
      node._uiLocation = {
        top: graph._nodes[node.dataId].y - 20 + 'px', // 20 = half of node height
        left: graph._nodes[node.dataId].x - 90 + 'px' // 90 = half of node width
      };
    });
  }
}

angular.module(PKG.name + '.feature.tracker')
  .service('myLineageService', myLineageService);
