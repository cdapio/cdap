/*
 * Copyright © 2020 Cask Data, Inc.
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

import { loginIfRequired, getGenericEndpoint, getArtifactsPoll, dataCy, setNewSchemaEditor } from '../helpers';
import { DEFAULT_GCP_PROJECTID, DEFAULT_GCP_SERVICEACCOUNT_PATH } from '../support/constants';
import { INodeInfo, INodeIdentifier } from '../typings';

let headers = {};

// We don't have preview enabled in distributed mode.
// TODO(CDAP-16620) To enable preview in distributed mode. Once it is enabled we can remove.
let skipPreviewTests = false;
// @ts-ignore "cy.state" is not in the "cy" type
const getMochaContext = () => cy.state('runnable').ctx;
const skip = () => {
  const ctx = getMochaContext();
  return ctx.skip();
};

// Pipeline and plugin info
const JOINER_PIPELINE_NAME = 'joiner_pipeline_name';
const TEST_BQ_DATASET_PROJECT = 'datasetproject';
const TEST_DATASET = 'joiner_test';
const TABLE1 = 'ninety_nine_cols';
const TABLE2 = 'two_hundred_cols';
const TABLE3 = 'sink_table';
const TABLE_FIELDS = ['string_field_0', 'string_field_1', 'string_field_2'];
const FIELD_ALIASES = ['field'];

const sourceNode1: INodeInfo = { nodeName: 'BigQueryTable', nodeType: 'batchsource' };
const sourceNodeId1: INodeIdentifier = { ...sourceNode1, nodeId: '0' };
const sourceNode2: INodeInfo = { nodeName: 'BigQueryTable', nodeType: 'batchsource' };
const sourceNodeId2: INodeIdentifier = { ...sourceNode2, nodeId: '1' };
const joinerNode: INodeInfo = { nodeName: 'Joiner', nodeType: 'batchjoiner' };
const joinerNodeId: INodeIdentifier = { ...joinerNode, nodeId: '2' };
const sinkNode: INodeInfo = { nodeName: 'BigQueryTable', nodeType: 'batchsink' };
const sinkNodeId: INodeIdentifier = { ...sinkNode, nodeId: '3' };

// Preview messaging
const runPreviewMsg = 'Run preview to generate preview data';
const noOutputDataMsg = 'Input records have not been generated';
const successMsg = 'The preview of the pipeline has completed successfully.';

describe('Creating pipeline with joiner in pipeline studio', () => {
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
    setNewSchemaEditor();
    getArtifactsPoll(headers);
  });
  afterEach(() => {
    cy.clearLocalStorage();
  });

  after(() => {
    // Delete the pipeline to clean up
    cy.cleanup_pipelines(headers, JOINER_PIPELINE_NAME);
  });

  it('Should be able to build a complex pipeline with joiner widget', () => {
    cy.visit('/pipelines/ns/default/studio');
    const closeButton = '[data-testid="close-config-popover"]';
    const getSchemaBtn = dataCy('get-schema-btn');
    const cleanUpButton = dataCy('pipeline-clean-up-graph-control');
    const fitToScreenButton = dataCy('pipeline-fit-to-screen-control');

    // Build pipeline with two BQ sources, Joiner, and BQ sink
    const source1Properties = {
      referenceName: 'BQ_Source1',
      project: DEFAULT_GCP_PROJECTID,
      datasetProject: TEST_BQ_DATASET_PROJECT,
      dataset: TEST_DATASET,
      table: TABLE1,
      serviceFilePath: DEFAULT_GCP_SERVICEACCOUNT_PATH,
    };

    const source2Properties = {
      referenceName: 'BQ_Source2',
      project: DEFAULT_GCP_PROJECTID,
      datasetProject: TEST_BQ_DATASET_PROJECT,
      dataset: TEST_DATASET,
      table: TABLE2,
      serviceFilePath: DEFAULT_GCP_SERVICEACCOUNT_PATH,
    };

    const sinkProperties = {
      referenceName: 'BQ_Sink',
      project: DEFAULT_GCP_PROJECTID,
      datasetProject: TEST_BQ_DATASET_PROJECT,
      dataset: TEST_DATASET,
      table: TABLE3,
      serviceFilePath: DEFAULT_GCP_SERVICEACCOUNT_PATH,
    };

    cy.add_node_to_canvas(sourceNode1);
    cy.add_node_to_canvas(sourceNode2);

    cy.open_analytics_panel();
    cy.add_node_to_canvas(joinerNode);

    cy.open_sink_panel();
    cy.add_node_to_canvas(sinkNode);

    cy.get(cleanUpButton).click();
    cy.get(fitToScreenButton).click();

    cy.connect_two_nodes(sourceNodeId1, joinerNodeId, getGenericEndpoint);
    cy.connect_two_nodes(sourceNodeId2, joinerNodeId, getGenericEndpoint);
    cy.connect_two_nodes(joinerNodeId, sinkNodeId, getGenericEndpoint);

    cy.get(cleanUpButton).click();
    cy.get(fitToScreenButton).click();

    // configure the plugin properties for BigQuery source 1
    cy.get(`${dataCy('plugin-node-BigQueryTable-batchsource-0')} .node .node-configure-btn`)
      .invoke('show')
      .click();

    cy.get(`input${dataCy('referenceName')}`).type(source1Properties.referenceName);
    cy.get(`input${dataCy('project')}`)
      .clear()
      .type(source1Properties.project);
    cy.get(`input${dataCy('dataset')}`).type(source1Properties.dataset);
    cy.get(`input${dataCy('table')}`).type(source1Properties.table);
    cy.get(`input${dataCy('serviceFilePath')}`)
      .clear()
      .type(source1Properties.serviceFilePath);

    // Validate and check fields for source1
    cy.get(dataCy('plugin-properties-validate-btn')).click();
    cy.get(dataCy('plugin-validation-success-msg')).should('exist');

    cy.get(dataCy('plugin-output-schema-container')).scrollIntoView();

    TABLE_FIELDS.forEach((field) => {
      cy.get(`[data-cy="${field}-schema-field"]`).should('exist');
    });

    cy.get(closeButton).click();

    // configure the plugin properties for BigQuery source 2
    cy.get('[data-cy="plugin-node-BigQueryTable-batchsource-1"] .node .node-configure-btn')
      .invoke('show')
      .click();

    cy.get('input[data-cy="referenceName"]').type(source2Properties.referenceName);
    cy.get('input[data-cy="project"]')
      .clear()
      .type(source2Properties.project);
    cy.get('input[data-cy="dataset"]').type(source2Properties.dataset);
    cy.get('input[data-cy="table"]').type(source2Properties.table);
    cy.get('input[data-cy="serviceFilePath"]')
      .clear()
      .type(source2Properties.serviceFilePath);

    // Use get Schema button to check fields for source2
    cy.get(getSchemaBtn).click();
    cy.get(getSchemaBtn).contains('Get Schema');

    cy.get('[data-cy="plugin-output-schema-container"]').scrollIntoView();

    TABLE_FIELDS.forEach((field) => {
      cy.get(`[data-cy="${field}-schema-field"]`).should('exist');
    });

    cy.get(closeButton).click();

    // check if schemas propagated correctly to the joiner widget
    cy.get('[data-cy="plugin-node-Joiner-batchjoiner-2"] .node .node-configure-btn')
      .invoke('show')
      .click();

    // Check for both input stages in properties
    cy.get('[data-cy="BigQueryTable-input-stage"]').should('exist');
    cy.get('[data-cy="BigQueryTable2-input-stage"]').should('exist');

    // Check the fields for each input
    cy.get('[data-cy="BigQueryTable-stage-expansion-panel"]').click();

    TABLE_FIELDS.forEach((field) => {
      cy.get(`[data-cy="${field}-field-selector-name"]`).should('exist');
    });

    // Clear default schema selection and change alias of field 1
    cy.get(dataCy('schema-select-btn')).click();
    cy.get(dataCy('select-none-option')).click();
    cy.get(dataCy(`${TABLE_FIELDS[0]}-field-selector-checkbox`)).within(() => {
      cy.get('[type="checkbox"]').check();
    });

    cy.get(
      `${dataCy('BigQueryTable-stage-expansion-panel')} ${dataCy(
        `${TABLE_FIELDS[0]}-field-selector-alias-textbox`
      )} input`
    )
      .clear()
      .type(FIELD_ALIASES[0]);

    cy.get(dataCy('BigQueryTable2-stage-expansion-panel')).click();

    TABLE_FIELDS.forEach((field) => {
      cy.get(dataCy(`${field}-field-selector-name`)).should('exist');
    });

    // Check the output schema
    cy.get(getSchemaBtn).click();
    cy.get(getSchemaBtn).contains('Get Schema');

    cy.get(dataCy('plugin-output-schema-container'))
      .scrollIntoView()
      .within(() => {
        FIELD_ALIASES.forEach((field) => {
          cy.get(dataCy(`${field}-schema-field`)).should('exist');
        });
      });

    cy.get(closeButton).click();

    // configure the plugin properties for BigQuery sink
    cy.get(`${dataCy('plugin-node-BigQueryTable-batchsink-3')} .node .node-configure-btn`)
      .invoke('show')
      .click();

    cy.get('input[data-cy="referenceName"]').type(sinkProperties.referenceName);
    cy.get('input[data-cy="project"]')
      .clear()
      .type(sinkProperties.project);
    cy.get('input[data-cy="dataset"]').type(sinkProperties.dataset);
    cy.get('input[data-cy="table"]').type(sinkProperties.table);
    cy.get('input[data-cy="serviceFilePath"]')
      .clear()
      .type(sinkProperties.serviceFilePath);

    cy.get(dataCy('plugin-output-schema-container')).within(() => {
      FIELD_ALIASES.forEach((field) => {
        cy.get(dataCy(`${field}-schema-field`)).should('exist');
      });
    });

    cy.get(closeButton).click();
  });

  it('Should render Get Schema button', () => {
    cy.open_node_property(joinerNodeId);
    cy.get(dataCy('get-schema-btn')).should('exist');
  });

  it('Should still render Get Schema button when numPartitions is a macro', () => {
    cy.get(`input${dataCy('numPartitions')}`).type('${testing.macro}', {
      parseSpecialCharSequences: false,
    });
    cy.close_node_property();
    cy.open_node_property(joinerNodeId);
    cy.get(dataCy('get-schema-btn')).should('exist');
    cy.get(`input${dataCy('numPartitions')}`).clear();
    cy.close_node_property();
  });

  it('Should show appropriate message when preview has not been run yet', () => {
    cy.window().then((window) => {
      skipPreviewTests = window.CDAP_CONFIG.hydrator.previewEnabled !== true;
      if (skipPreviewTests) {
        skip();
      }
    });
    cy.get(dataCy('pipeline-preview-btn')).click();
    cy.get(dataCy(`${joinerNode.nodeName}-preview-data-btn`)).click();
    cy.contains(runPreviewMsg).should('be.visible');
    cy.close_node_property();
  });

  it('Should show appropriate message when preview has been stopped before data is generated', () => {
    cy.window().then((window) => {
      skipPreviewTests = window.CDAP_CONFIG.hydrator.previewEnabled !== true;
      if (skipPreviewTests) {
        skip();
      }
    });
    // Start and then immediately stop preview
    cy.get(dataCy('preview-top-run-btn')).click();
    cy.get(dataCy('stop-preview-btn')).click();
    cy.get(dataCy('preview-top-run-btn'), { timeout: 35000 }).should('exist');
    cy.get(dataCy(`plugin-node-BigQueryTable-batchsink-3`)).within(() => {
      cy.get(dataCy(`${sinkNode.nodeName}-preview-data-btn`)).click();
    });
    cy.contains(noOutputDataMsg).should('be.visible');
    cy.close_node_property();
  });

  it('Should show preview data with record view by default for sink', () => {
    cy.window().then((window) => {
      skipPreviewTests = window.CDAP_CONFIG.hydrator.previewEnabled !== true;
      if (skipPreviewTests) {
        skip();
      }
    });
    // Start and run preview
    cy.get(dataCy('preview-top-run-btn')).click();
    cy.get(dataCy('stop-preview-btn')).should('be.visible');
    cy.get(dataCy('preview-top-run-btn'), { timeout: 70000 }).should('exist');
    cy.get(dataCy(`plugin-node-BigQueryTable-batchsink-3`)).within(() => {
      cy.get(dataCy(`${sinkNode.nodeName}-preview-data-btn`)).click();
    });
    // Should be able to navigate records and toggle view
    cy.get(dataCy('toggle-Record'), { timeout: 15000 }).should('exist');
    cy.get(dataCy('fieldname-field')).should('be.visible');

    cy.get(dataCy('record-dropdown')).click();
    cy.contains('Record 3').click();
    cy.get(dataCy('value-string_81_1')).should('be.visible');
    cy.get(dataCy('previous-record-btn')).click();
    cy.get(dataCy('value-string_35_1')).should('be.visible');
    cy.get(dataCy('toggle-Record')).click();

    cy.get(dataCy('toggle-Table'), { timeout: 10000 }).should('be.visible');

    cy.close_node_property();
  });

  it('Should show preview data for all inputs for joiner', () => {
    cy.window().then((window) => {
      skipPreviewTests = window.CDAP_CONFIG.hydrator.previewEnabled !== true;
      if (skipPreviewTests) {
        skip();
      }
    });

    cy.get(dataCy(`plugin-node-Joiner-batchjoiner-2`)).within(() => {
      cy.get(dataCy(`${joinerNode.nodeName}-preview-data-btn`)).click();
    });
    cy.get(dataCy(`tab-head-${sourceNode1.nodeName}`)).should('be.visible');
    cy.get(dataCy(`tab-head-${sourceNode2.nodeName}2`)).should('be.visible');
    cy.close_node_property();
  });
});
