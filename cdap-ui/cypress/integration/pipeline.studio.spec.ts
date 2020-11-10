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
  dataCy,
  loginIfRequired,
  getGenericEndpoint,
  getConditionNodeEndpoint,
  getArtifactsPoll,
} from '../helpers';
import {
  DEFAULT_GCP_PROJECTID,
  DEFAULT_GCP_SERVICEACCOUNT_PATH,
  DEFAULT_BIGQUERY_DATASET,
  DEFAULT_BIGQUERY_TABLE,
} from '../support/constants';
import { INodeInfo, INodeIdentifier } from '../typings';
let headers = {};
const TEST_PIPELINE_NAME = 'pipeline_name';
const sourceNode: INodeInfo = { nodeName: 'BigQueryTable', nodeType: 'batchsource' };
const sourceNodeId: INodeIdentifier = { ...sourceNode, nodeId: '0' };
const transformNode: INodeInfo = { nodeName: 'Wrangler', nodeType: 'transform' };
const transformNodeId: INodeIdentifier = { ...transformNode, nodeId: '1' };
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
    cy.visit('/cdap', {
      onBeforeLoad: (win) => {
        win.sessionStorage.clear();
      },
    });
  });

  beforeEach(() => {
    getArtifactsPoll(headers);
  });

  after(() => {
    // Delete the pipeline to clean up
    cy.cleanup_pipelines(headers, TEST_PIPELINE_NAME);
  });

  it('Should renders pipeline studio correctly', () => {
    cy.visit('/pipelines/ns/default/studio');
  });

  it('Should render different types of nodes to the canvas', () => {
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
    const sinkNode: INodeInfo = { nodeName: 'BigQueryMultiTable', nodeType: 'batchsink' };
    const sinkNodeId: INodeIdentifier = { ...sinkNode, nodeId: '2' };

    cy.get('#diagram-container');
    cy.connect_two_nodes(sourceNodeId, transformNodeId, getGenericEndpoint);
    cy.connect_two_nodes(transformNodeId, sinkNodeId, getGenericEndpoint);
  });

  it('Should be able to build a complex pipeline', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.create_complex_pipeline();
  });

  it('Should configure plugin properties', () => {
    cy.visit('/pipelines/ns/default/studio');
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
    };
    cy.add_node_to_canvas(sourceNode);

    cy.open_sink_panel();
    cy.add_node_to_canvas(sinkNode1);

    cy.connect_two_nodes(sourceNodeId, sinkNodeId1, getGenericEndpoint);

    // Open Bigquery source properties modal
    cy.get('[data-cy="plugin-node-BigQueryTable-batchsource-0"] .node .node-configure-btn')
      .invoke('show')
      .click();

    // Configure properties for Bigquery source
    cy.get('input[data-cy="referenceName"]').type(sourceProperties.referenceName);
    cy.get('input[data-cy="project"]')
      .clear()
      .type(sourceProperties.project);
    cy.get('input[data-cy="datasetProject"]').type(sourceProperties.datasetProject);
    cy.get('input[data-cy="dataset"]').type(sourceProperties.dataset);
    cy.get('input[data-cy="table"]').type(sourceProperties.table);
    cy.get('input[data-cy="serviceFilePath"]')
      .clear()
      .type(sourceProperties.serviceFilePath);
    // close the modal
    cy.get('[data-testid="close-config-popover"]').click();

    // Open Bigquery sink properties modal
    cy.get('[data-cy="plugin-node-BigQueryTable-batchsink-1"] .node .node-configure-btn')
      .invoke('show')
      .click();

    // Configure properties for Bigquery sink
    cy.get('input[data-cy="referenceName"]').type(sinkProperties.referenceName);
    cy.get('input[data-cy="dataset"]').type(sinkProperties.dataset);
    cy.get('[data-cy="switch-truncateTable"]').click();
    cy.get('input[data-cy="project"]')
      .clear()
      .type(sinkProperties.project);
    cy.get('input[data-cy="serviceFilePath"]')
      .clear()
      .type(sinkProperties.serviceFilePath);
    cy.get('input[data-cy="table"]')
      .clear()
      .type(sinkProperties.table);
    // close the modal
    cy.get('[data-testid="close-config-popover"]').click();

    // set name and description for pipeline
    cy.get('[data-cy="pipeline-metadata"]').click();
    cy.get('#pipeline-name-input').type(TEST_PIPELINE_NAME);
    cy.get('[data-cy="pipeline-description-input"]').type('Sample pipeline description');
    cy.get('[data-cy="pipeline-metadata-ok-btn"]').click();
    // Export the pipeline to validate if configure plugins reflect correctly.
    cy.get_pipeline_json().then((pipelineConfig) => {
      const stages = pipelineConfig.config.stages;
      const sourcePropertiesFromNode = stages.find(
        (stage) => stage.plugin.name === 'BigQueryTable' && stage.plugin.type === 'batchsource'
      ).plugin.properties;
      const sinkPropertiesFromNode = stages.find(
        (stage) => stage.plugin.name === 'BigQueryTable' && stage.plugin.type === 'batchsink'
      ).plugin.properties;
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
    });
    cy.get('[data-testid="deploy-pipeline"]').click();
    cy.url({ timeout: 60000 }).should('include', `/view/${TEST_PIPELINE_NAME}`);
  });

  it('should show the number of unfilled fields on a source', () => {
    // Go to Pipelines studio
    cy.visit('/pipelines/ns/default/studio');

    const sourceNode: INodeInfo = { nodeName: 'BigQueryTable', nodeType: 'batchsource' };
    cy.add_node_to_canvas(sourceNode);

    cy.get(dataCy('plugin-node-BigQueryTable-batchsource-0')).within(() => {
      cy.get(dataCy('node-error-count')).should('have.text', '3');
    });
  });

  it('should save and reload a pipeline', () => {
    const REFERENCE_NAME = 'TestReferenceName';
    const DATASET = 'TestDataset';
    const TABLE = 'TestTable';
    // Go to Pipelines studio
    cy.visit('/pipelines/ns/default/studio');
    cy.create_simple_pipeline();

    cy.get('.pipeline-name').click();
    cy.get('#pipeline-name-input')
      .type(TEST_PIPELINE_NAME)
      .type('{enter}');

    cy.get(dataCy('pipeline-draft-save-btn')).click();

    // go to pipeline listing and select draft
    cy.visit('/cdap/ns/default/pipelines');
    cy.get(dataCy('pipeline-list-view-header'))
      .contains('Drafts')
      .click();
    cy.get(dataCy('draft-pipeline-table'))
      .contains(TEST_PIPELINE_NAME)
      .click();

    // Configure source
    cy.get(dataCy('plugin-node-BigQueryTable-batchsource-0')).within(() => {
      cy.get(dataCy('node-properties-btn'))
        .invoke('show')
        .click();
    });

    function setFieldText(id, value) {
      cy.get(dataCy(id)).within(() => {
        cy.get('input')
          .clear()
          .type(value);
      });
    }

    setFieldText('referenceName', REFERENCE_NAME);
    setFieldText('dataset', DATASET);
    setFieldText('table', TABLE);

    cy.get('[data-testid="close-config-popover"]').click();

    cy.get(dataCy('pipeline-draft-save-btn')).click();

    // go to pipeline listing and select draft (again)
    cy.visit('/cdap/ns/default/pipelines');
    cy.get(dataCy('pipeline-list-view-header'))
      .contains('Drafts')
      .click();
    cy.get(dataCy('draft-pipeline-table'))
      .contains(TEST_PIPELINE_NAME)
      .click();

    cy.get_pipeline_json().then((pipelineConfig) => {
      const sourceProperties = pipelineConfig.config.stages[0].plugin.properties;
      expect(sourceProperties.referenceName).eq(REFERENCE_NAME);
      expect(sourceProperties.dataset).eq(DATASET);
      expect(sourceProperties.table).eq(TABLE);
    });
  });
  it('Should honor user choice of empty value over default value for a plugin property', () => {
    const sourceProperties = {
      referenceName: 'BQ_Source',
      project: DEFAULT_GCP_PROJECTID,
      datasetProject: DEFAULT_GCP_PROJECTID,
      dataset: DEFAULT_BIGQUERY_DATASET,
      table: DEFAULT_BIGQUERY_TABLE,
      serviceFilePath: DEFAULT_GCP_SERVICEACCOUNT_PATH,
    };
    cy.visit('/pipelines/ns/default/studio');
    cy.add_node_to_canvas(sourceNode);
    cy.open_node_property(sourceNodeId);
    // This arbitrary wait time is to make sure the widget json default values
    // are loaded before checking for them.
    cy.wait(2000);
    cy.get('input[data-cy="project"]')
      .invoke('val')
      .then((val) => expect(val).equals('auto-detect'));
    cy.get('input[data-cy="serviceFilePath"]')
      .invoke('val')
      .then((val) => expect(val).equals('auto-detect'));
    cy.get('input[data-cy="project"]').clear();
    cy.get('input[data-cy="project"]')
      .invoke('val')
      .then((val) => expect(val).equals(''));
  });
});
