/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License'); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
*/

import {
  loginIfRequired,
  getGenericEndpoint,
  getConditionNodeEndpoint,
  getArtifactsPoll
} from '../helpers';
import { INodeInfo, INodeIdentifier } from '../typings';
let headers = {};
describe('Pipeline Studio', () => {
  // Uses API call to login instead of logging in manually through UI
  before(() => {
    loginIfRequired().then(() => {
      cy.getCookie('CDAP_Auth_Token').then((cookie) => {
        if (!cookie) {
          return;
        }
        headers = {
          Authorization: 'Bearer ' + cookie.value,
        };
      });
    });
  });

  beforeEach(() => {
    getArtifactsPoll(headers);
  });

  it('Should renders pipeline studio correctly', () => {
    cy.visit('/pipelines/ns/default/studio');
  });

  it('Should render different types of nodes to the canvas', () => {
    const sourceNode: INodeInfo = { nodeName: 'BigQueryTable', nodeType: 'batchsource' };
    const sourceNodeId: INodeIdentifier = { ...sourceNode, nodeId: '0' };
    const transformNode: INodeInfo = { nodeName: 'Wrangler', nodeType: 'transform' };
    const transformNodeId: INodeIdentifier = { ...transformNode, nodeId: '1' };
    const sinkNode: INodeInfo = { nodeName: 'BigQueryMultiTable', nodeType: 'batchsink' };
    const sinkNodeId: INodeIdentifier = { ...sinkNode, nodeId: '2' };
    cy.add_node_to_canvas(sourceNode);

    cy.open_transform_panel();
    cy.add_node_to_canvas(transformNode);

    cy.open_sink_panel();
    cy.add_node_to_canvas(sinkNode);

    cy.get('#diagram-container');
    cy.move_node(sourceNodeId, 50, 50);
    cy.move_node(transformNodeId, 100, 50);
    cy.move_node(sinkNodeId, 150, 50);
  });

  it('Should be able to connect two nodes in the canvas', () => {
    const sourceNode: INodeInfo = { nodeName: 'BigQueryTable', nodeType: 'batchsource' };
    const sourceNodeId: INodeIdentifier = { ...sourceNode, nodeId: '0' };
    const transformNode: INodeInfo = { nodeName: 'Wrangler', nodeType: 'transform' };
    const transformNodeId: INodeIdentifier = { ...transformNode, nodeId: '1' };
    const sinkNode: INodeInfo = { nodeName: 'BigQueryMultiTable', nodeType: 'batchsink' };
    const sinkNodeId: INodeIdentifier = { ...sinkNode, nodeId: '2' };

    cy.get('#diagram-container');
    cy.connect_two_nodes(sourceNodeId, transformNodeId, getGenericEndpoint);
    cy.connect_two_nodes(transformNodeId, sinkNodeId, getGenericEndpoint);
  });

  it('Should be able to build a complex pipeline', () => {
    cy.visit('/pipelines/ns/default/studio');
    // Two BigQuery sources
    const sourceNode1: INodeInfo = { nodeName: 'BigQueryTable', nodeType: 'batchsource' };
    const sourceNodeId1: INodeIdentifier = { ...sourceNode1, nodeId: '0' };
    const sourceNode2: INodeInfo = { nodeName: 'BigQueryTable', nodeType: 'batchsource' };
    const sourceNodeId2: INodeIdentifier = { ...sourceNode2, nodeId: '1' };

    // Two javascript transforms 
    const transformNode1: INodeInfo = { nodeName: 'JavaScript', nodeType: 'transform' };
    const transformNodeId1: INodeIdentifier = { ...transformNode1, nodeId: '2' };
    const transformNode2: INodeInfo = { nodeName: 'JavaScript', nodeType: 'transform' };
    const transformNodeId2: INodeIdentifier = { ...transformNode2, nodeId: '3' };


    // One joiner
    const joinerNode: INodeInfo = { nodeName: 'Joiner', nodeType: 'batchjoiner' };
    const joinerNodeId: INodeIdentifier = { ...joinerNode, nodeId: '4' };

    // One condition node
    const conditionNode: INodeInfo = { nodeName: 'Conditional', nodeType: 'condition' }
    const conditionNodeId: INodeIdentifier = { ...conditionNode, nodeId: '5' };

    // Two BigQuery sinks
    const sinkNode1: INodeInfo = { nodeName: 'BigQueryMultiTable', nodeType: 'batchsink' };
    const sinkNodeId1: INodeIdentifier = { ...sinkNode1, nodeId: '6' };
    const sinkNode2: INodeInfo = { nodeName: 'BigQueryMultiTable', nodeType: 'batchsink' };
    const sinkNodeId2: INodeIdentifier = { ...sinkNode2, nodeId: '7' };

    cy.add_node_to_canvas(sourceNode1);
    cy.add_node_to_canvas(sourceNode2);

    cy.open_transform_panel();
    cy.add_node_to_canvas(transformNode1);
    cy.add_node_to_canvas(transformNode2);

    cy.open_analytics_panel();
    cy.add_node_to_canvas(joinerNode);

    cy.open_condition_and_actions_panel();
    cy.add_node_to_canvas(conditionNode);

    cy.open_sink_panel();
    cy.add_node_to_canvas(sinkNode1);
    cy.add_node_to_canvas(sinkNode2);

    cy.get('[data-cy="pipeline-clean-up-graph-control"]').click();
    cy.get('[data-cy="pipeline-fit-to-screen-control"]').click();

    cy.connect_two_nodes(sourceNodeId1, transformNodeId1, getGenericEndpoint);
    cy.connect_two_nodes(sourceNodeId2, transformNodeId2, getGenericEndpoint);

    cy.connect_two_nodes(transformNodeId1, joinerNodeId, getGenericEndpoint);
    cy.connect_two_nodes(transformNodeId2, joinerNodeId, getGenericEndpoint);

    cy.connect_two_nodes(joinerNodeId, conditionNodeId, getGenericEndpoint);

    cy.connect_two_nodes(conditionNodeId, sinkNodeId1, getConditionNodeEndpoint, { condition: true });
    cy.connect_two_nodes(conditionNodeId, sinkNodeId2, getConditionNodeEndpoint, { condition: false });

    cy.get('[data-cy="pipeline-clean-up-graph-control"]').click();
    cy.get('[data-cy="pipeline-fit-to-screen-control"]').click();

    cy.compareSnapshot('pipeline_with_condition');
  });
});
