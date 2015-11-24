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

class HydratorService {
  constructor(GLOBALS, MyDAGFactory, uuid) {
    this.GLOBALS = GLOBALS;
    this.MyDAGFactory = MyDAGFactory;
    this.uuid = uuid;
  }

  getNodesAndConnectionsFromConfig(pipeline) {
    let nodes = [];
    let connections = [];

    let artifact = this.GLOBALS.pluginTypes[pipeline.artifact.name];

    let source = angular.copy(pipeline.config.source);
    let transforms = angular.copy(pipeline.config.transforms)
      .map( node => {
        node.type = artifact.transform;
        if (!node.id) {
          node.id = node.name + '-' + node.type + '-' + this.uuid.v4();
        }
        node.icon = this.MyDAGFactory.getIcon(node.name);
        return node;
      });
    let sinks = angular.copy(pipeline.config.sinks)
      .map( node => {
        node.type = artifact.sink;
        if (!node.id) {
          node.id = node.name + '-' + node.type + '-' + this.uuid.v4();
        }
        node.icon = this.MyDAGFactory.getIcon(node.name);
        return node;
      });

    source.type = artifact.source;
    source.icon = this.MyDAGFactory.getIcon(source.name);
    if (!source.id) {
      source.id = source.name + '-' + source.type + '-' + this.uuid.v4();
    }
    // replace with backend id
    nodes.push(source);
    nodes = nodes.concat(transforms);
    nodes = nodes.concat(sinks);


    let prevId;

    if (transforms.length === 0) {
      prevId = source.id;
    } else {
      prevId = transforms[0].id;
      // Set Connections
      connections.push({
        source: source.id,
        target: transforms[0].id
      });
    }

    for (let i = 1; i < transforms.length; i++) {
      connections.push({
        source: prevId,
        target: transforms[i].id
      });
      prevId = transforms[i].id;
    }

    for (let i = 0; i < sinks.length; i++) {
      connections.push({
        source: prevId,
        target: sinks[i].id
      });
    }

    // Obtaining layout of graph with Dagre
    var graph = this.MyDAGFactory.getGraphLayout(nodes, connections);
    angular.forEach(nodes, function (node) {
      node._uiPosition = {
        'top': graph._nodes[node.id].y + 'px' ,
        'left': graph._nodes[node.id].x + 'px'
      };
    });

    return {
      nodes: nodes,
      connections: connections
    };
  }
}
HydratorService.$inject = ['GLOBALS', 'MyDAGFactory', 'uuid'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('HydratorService', HydratorService);
