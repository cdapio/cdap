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
import { DEFAULT_GCP_PROJECTID, DEFAULT_GCP_SERVICEACCOUNT_PATH, DEFAULT_BIGQUERY_DATASET, DEFAULT_BIGQUERY_TABLE } from '../support/constants';
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

  it('Should configure plugin properties', () => {

    cy.visit('/pipelines/ns/default/studio');
    const TEST_PIPELINE_NAME = 'pipeline_name';
    const sourceNode1: INodeInfo = { nodeName: 'BigQueryTable', nodeType: 'batchsource' };
    const sourceNodeId1: INodeIdentifier = { ...sourceNode1, nodeId: '0' };
    const sourceProperties = {
      referenceName: 'BQ_Source',
      project: DEFAULT_GCP_PROJECTID,
      datasetProject: DEFAULT_GCP_PROJECTID,
      dataset: DEFAULT_BIGQUERY_DATASET,
      table: DEFAULT_BIGQUERY_TABLE,
      serviceFilePath: DEFAULT_GCP_SERVICEACCOUNT_PATH,
    };
    const sinkNode1: INodeInfo = { nodeName: 'BigQueryTable', nodeType: 'batchsink' };
    const sinkNodeId1: INodeIdentifier = { ...sinkNode1, nodeId: '1' };
    const newSinkTable = `${DEFAULT_BIGQUERY_TABLE}_${Date.now()}`;
    const sinkProperties = {
      referenceName: 'BQ_Sink',
      dataset: DEFAULT_BIGQUERY_DATASET,
      table: newSinkTable,
      truncateTable: 'true',
      project: DEFAULT_GCP_PROJECTID,
      serviceFilePath: DEFAULT_GCP_SERVICEACCOUNT_PATH,
    }
    cy.add_node_to_canvas(sourceNode1);

    cy.open_sink_panel();
    cy.add_node_to_canvas(sinkNode1);

    cy.connect_two_nodes(sourceNodeId1, sinkNodeId1, getGenericEndpoint);

    // Open Bigquery source properties modal
    cy.get('[data-cy="plugin-node-BigQueryTable-batchsource-0"] .node .node-configure-btn')
      .invoke('show').click();

    // Configure properties for Bigquery source
    cy.get('input[data-cy="referenceName"]').type(sourceProperties.referenceName);
    cy.get('input[data-cy="project"]').clear().type(sourceProperties.project);
    cy.get('input[data-cy="datasetProject"]').type(sourceProperties.datasetProject);
    cy.get('input[data-cy="dataset"]').type(sourceProperties.dataset);
    cy.get('input[data-cy="table"]').type(sourceProperties.table);
    cy.get('input[data-cy="serviceFilePath"]').clear().type(sourceProperties.serviceFilePath);
    // close the modal
    cy.get('[data-testid="close-config-popover"]').click();

    // Open Bigquery sink properties modal
    cy.get('[data-cy="plugin-node-BigQueryTable-batchsink-1"] .node .node-configure-btn').invoke('show').click();

    // Configure properties for Bigquery sink
    cy.get('input[data-cy="referenceName"]').type(sinkProperties.referenceName);
    cy.get('input[data-cy="dataset"]').type(sinkProperties.dataset);
    cy.get('[data-cy="switch-truncateTable"]').click();
    cy.get('input[data-cy="project"]').clear().type(sinkProperties.project)
    cy.get('input[data-cy="serviceFilePath"]').clear().type(sinkProperties.serviceFilePath);
    cy.get('input[data-cy="table"]').clear().type(sinkProperties.table);
    // close the modal
    cy.get('[data-testid="close-config-popover"]').click();

    // set name and description for pipeline
    cy.get('[data-cy="pipeline-metadata"]').click();
    cy.get('#pipeline-name-input').type(TEST_PIPELINE_NAME);
    cy.get('[data-cy="pipeline-description-input"]').type('Sample pipeline description');
    cy.get('[data-cy="pipeline-metadata-ok-btn"]').click();
    // Export the pipeline to validate if configure plugins reflect correctly.
    cy.get('[data-cy="pipeline-export-btn"]').click();
    cy.get('textarea[data-cy="pipeline-export-json-container"]').invoke('val').then(va => {
      if (typeof va !== 'string') {
        throw new Error('Unable to get pipeline config');
      }
      let pipelineConfig;
      try {
        pipelineConfig = JSON.parse(va);
      } catch (e) {
        throw new Error('Invalid pipeline config');
      }
      const stages = pipelineConfig.config.stages;
      const sourcePropertiesFromNode = stages.find(stage => stage.plugin.name === 'BigQueryTable' && stage.plugin.type === 'batchsource').plugin.properties;
      const sinkPropertiesFromNode = stages.find(stage => stage.plugin.name === 'BigQueryTable' && stage.plugin.type === 'batchsink').plugin.properties;
      expect(sourcePropertiesFromNode.referenceName).equals(sourceProperties.referenceName);
      expect(sourceProperties.project).equals(sourceProperties.project);
      expect(sourceProperties.datasetProject).equals(sourceProperties.datasetProject);
      expect(sourceProperties.dataset).equals(sourceProperties.dataset);
      expect(sourceProperties.table).equals(sourceProperties.table);
      expect(sourceProperties.serviceFilePath).equals(sourceProperties.serviceFilePath);

      expect(sinkPropertiesFromNode.referenceName).equal(sinkProperties.referenceName);
      expect(sinkPropertiesFromNode.table).equal(sinkProperties.table);
      expect(sinkPropertiesFromNode.dataset).equal(sinkProperties.dataset);
      expect(sinkPropertiesFromNode.truncateTable).equal(sinkProperties.truncateTable);
      expect(sinkPropertiesFromNode.project).equal(sinkProperties.project);
      expect(sinkPropertiesFromNode.serviceFilePath).equal(sinkProperties.serviceFilePath);

      cy.get('[data-cy="export-pipeline-close-modal-btn"]').click();
      cy.get('[data-testid="deploy-pipeline"]').click();
      cy.url({ timeout: 60000 }).should('include', `/view/${TEST_PIPELINE_NAME}`);
    });
  });
});
