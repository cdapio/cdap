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

import { INodeInfo, INodeIdentifier } from '../typings';
import { loginIfRequired, getArtifactsPoll, dataCy, setNewSchemaEditor } from '../helpers';
import {
  DEFAULT_GCP_PROJECTID,
  DEFAULT_GCP_SERVICEACCOUNT_PATH,
  DEFAULT_BIGQUERY_DATASET,
  DEFAULT_BIGQUERY_TABLE,
} from '../support/constants';

let headers = {};
const PIPELINE_NAME = `Test_macros_pipeline-${Date.now()}`;

describe('Output Schema', () => {
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
    setNewSchemaEditor();
  });

  afterEach(() => {
    cy.clearLocalStorage();
  });

  const schemaFieldSuffix = '-schema-field';

  const projection: INodeInfo = { nodeName: 'Projection', nodeType: 'transform' };
  const projectionId: INodeIdentifier = { ...projection, nodeId: '0' };
  const addButton = dataCy('schema-row-add-button');
  const removeButton = dataCy('schema-row-remove-button');
  const schemaField = dataCy(schemaFieldSuffix);
  const schemaRow1 = dataCy('schema-row-0');
  const schemaRow2 = dataCy('schema-row-1');
  const schemaRow3 = dataCy('schema-row-2');

  beforeEach(() => {
    setNewSchemaEditor();
    getArtifactsPoll(headers);
  });

  afterEach(() => {
    cy.clearLocalStorage();
  });

  after(() => {
    cy.cleanup_pipelines(headers, PIPELINE_NAME);
  });

  it('Should initialize with 1 empty row', () => {
    cy.visit('/pipelines/ns/default/studio');

    // add plugin to canvas
    cy.open_transform_panel();
    cy.add_node_to_canvas(projection);

    cy.open_node_property(projectionId);

    cy.get(schemaRow1).should('exist');
  });

  it('Should be able to add a new row', () => {
    cy.get(`${schemaRow1} ${schemaField}`).type('field1');
    cy.get(`${schemaRow1} ${addButton}`).click();

    cy.get(`${schemaRow2}`).should('exist');
    cy.get(`${schemaRow2} ${schemaField}`).should('have.focus');
    cy.get(`${schemaRow2} ${schemaField}`).type('another_field');
  });

  it('Should add new row directly underneath the current row', () => {
    cy.get(`${schemaRow1} ${addButton}`).click();
    cy.get(`${schemaRow2}`).should('exist');
    cy.get(`${schemaRow2} ${schemaField}`).should('have.focus');
    cy.get(`${schemaRow2} ${schemaField}`).type('middle');

    cy.get(`${schemaRow3} ${dataCy('another_field' + schemaFieldSuffix)}`)
      .invoke('val')
      .then((val) => {
        expect(val).equals('another_field');
      });
  });

  it('Should be able to remove row', () => {
    cy.get(`${schemaRow2} ${removeButton}`).click();

    cy.get(`${schemaRow3}`).should('not.exist');

    cy.get(`${schemaRow1} ${dataCy('field1' + schemaFieldSuffix)}`)
      .invoke('val')
      .then((val) => {
        expect(val).equals('field1');
      });

    cy.get(`${schemaRow2} ${dataCy('another_field' + schemaFieldSuffix)}`)
      .invoke('val')
      .then((val) => {
        expect(val).equals('another_field');
      });
  });

  it('Should retain value when node config is reopened', () => {
    cy.close_node_property();
    cy.open_node_property(projectionId);

    cy.get(`${schemaRow1} ${dataCy('field1' + schemaFieldSuffix)}`)
      .invoke('val')
      .then((val) => {
        expect(val).equals('field1');
      });

    cy.get(`${schemaRow2} ${dataCy('another_field' + schemaFieldSuffix)}`)
      .invoke('val')
      .then((val) => {
        expect(val).equals('another_field');
      });
  });

  it('Should work if the output schema is a macro', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.create_simple_pipeline().then(({ sourceNodeId, transformNodeId, sinkNodeId }) => {
      const sourceProperties = {
        referenceName: 'BQ_Source',
        project: DEFAULT_GCP_PROJECTID,
        datasetProject: DEFAULT_GCP_PROJECTID,
        dataset: DEFAULT_BIGQUERY_DATASET,
        table: DEFAULT_BIGQUERY_TABLE,
        serviceFilePath: DEFAULT_GCP_SERVICEACCOUNT_PATH,
      };
      const newSinkTable = `${DEFAULT_BIGQUERY_TABLE}_${Date.now()}`;
      const sinkProperties = {
        referenceName: 'BQ_Sink',
        dataset: DEFAULT_BIGQUERY_DATASET,
        table: newSinkTable,
        truncateTable: 'true',
        project: DEFAULT_GCP_PROJECTID,
        serviceFilePath: DEFAULT_GCP_SERVICEACCOUNT_PATH,
      };
      cy.open_node_property(sourceNodeId);
      cy.get('[data-cy="schema-action-button"] button').click();
      cy.get('[data-cy="toggle-schema-editor"]').click();
      cy.get('#macro-input-schema').type('source_output_schema');
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
      cy.close_node_property();

      cy.open_node_property(transformNodeId);
      cy.get('[data-cy="macro-input-schema"]').contains('${source_output_schema}');
      cy.get('[data-cy="schema-row-0"] input[type="text"]').type('name');
      cy.get('[data-cy="schema-row-add-button"]').click();
      cy.get('[data-cy="schema-row-1"] input[type="text"]').type('email');
      cy.get('[data-cy="schema-row-1"] [data-cy="schema-row-add-button"]').click();
      cy.close_node_property();

      cy.open_node_property(sinkNodeId);
      cy.get('[data-cy="plugin-input-schema-container"] [data-cy="name-schema-field"]').should(
        'have.value',
        'name'
      );
      cy.get('[data-cy="plugin-input-schema-container"] [data-cy="email-schema-field"]').should(
        'have.value',
        'email'
      );

      cy.get('input[data-cy="referenceName"]').type(sinkProperties.referenceName);
      cy.get('input[data-cy="dataset"]').type(sinkProperties.dataset);
      cy.get('[data-cy="switch-truncateTable"]').click();
      cy.get('input[data-cy="project"]')
        .clear()
        .type(sinkProperties.project);
      cy.get('input[data-cy="serviceFilePath"]')
        .clear()
        .type(sinkProperties.serviceFilePath);
      // close the modal
      cy.close_node_property();

      cy.get('[data-cy="pipeline-metadata"]').click();
      cy.get('#pipeline-name-input').type(PIPELINE_NAME);
      cy.get('[data-cy="pipeline-description-input"]').type('Sample pipeline description');
      cy.get('[data-cy="pipeline-metadata-ok-btn"]').click();

      cy.get('[data-testid="deploy-pipeline"]').click();
      cy.url({ timeout: 60000 }).should('include', `/view/${PIPELINE_NAME}`);

      // Wait to allow pipeline to finish rendering before checking node properties
      cy.wait(500);

      cy.open_node_property(sourceNodeId);
      cy.get('#macro-input-schema').should('have.value', '${source_output_schema}');
      cy.close_node_property();

      cy.open_node_property(transformNodeId);
      cy.get('[data-cy="macro-input-schema"]').contains('${source_output_schema}');
      cy.get('[data-cy="plugin-output-schema-container"] [data-cy="name-schema-field"]').should(
        'have.value',
        'name'
      );
      cy.get('[data-cy="plugin-output-schema-container"] [data-cy="email-schema-field"]').should(
        'have.value',
        'email'
      );
      cy.close_node_property();
    });
  });
});
