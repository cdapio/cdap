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

import { loginIfRequired, getArtifactsPoll, dataCy } from '../helpers';

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

const closeButton = '[data-testid="close-config-popover"]';

// Pipeline and plugin info
const JOINER_PIPELINE_NAME = `joiner_pipeline_${Date.now()}`;

const sourceNode1 = { nodeName: 'File', nodeType: 'batchsource', nodeId: '0' };
const sourceNode2 = { nodeName: 'File2', nodeType: 'batchsource', nodeId: '2' };
const joinerNode = { nodeName: 'Joiner', nodeType: 'batchjoiner', nodeId: '1' };
const sinkNode = { nodeName: 'TPFSAvro', nodeType: 'batchsink', nodeId: '3' };

// Preview messaging
const runPreviewMsg = 'Run preview to generate preview data';
const noDataMsg = 'Output records have not been generated';

describe('Running preview with joiner plugin in pipeline studio', () => {
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

  after(() => {
    // Delete the pipeline to clean up
    cy.cleanup_pipelines(headers, JOINER_PIPELINE_NAME);
  });

  it('Should be able to upload a pipeline with joiner widget', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.get('#resource-center-btn').click();
    cy.get('#create-pipeline-link').click();
    cy.url().should('include', '/studio');

    cy.upload_pipeline(
      'joiner_pipeline_v1-cdap-data-pipeline.json',
      '#pipeline-import-config-link > input[type="file"]'
    );
    // This is arbitrary. Right now we don't have a way to determine
    // if the upgrade check is done. Since this a standalone the assumption
    // is this won't take more than 10 seconds.
    cy.wait(10000);
    // Name pipeline
    cy.get('.pipeline-name').click();
    cy.get('#pipeline-name-input')
      .clear()
      .type(JOINER_PIPELINE_NAME)
      .type('{enter}');
  });

  it('Should show appropriate message when preview has not been run yet', () => {
    cy.window().then((window) => {
      skipPreviewTests = window.CDAP_CONFIG.hydrator.previewEnabled !== true;
    });
    if (skipPreviewTests) {
      skip();
    }
    cy.get(dataCy('pipeline-preview-btn')).click();
    cy.get(dataCy(`${joinerNode.nodeName}-preview-data-btn`)).click();
    cy.contains(runPreviewMsg).should('be.visible');
    cy.get(closeButton).click();
  });

  it('Should show appropriate message when preview has been stopped before data is generated', () => {
    cy.window().then((window) => {
      skipPreviewTests = window.CDAP_CONFIG.hydrator.previewEnabled !== true;
    });
    if (skipPreviewTests) {
      skip();
    }
    // Start and then immediately stop preview
    cy.get(dataCy('run-preview-btn')).click();
    cy.get(dataCy('stop-preview-btn')).click();
    // Check for successful stop banner
    cy.get(dataCy('valium-banner-hydrator')).within(() => {
      cy.get('.alert-success').should('be.visible');
    });
    cy.get(dataCy('run-preview-btn'), { timeout: 35000 }).should('exist');
    cy.get(
      dataCy(`plugin-node-${sourceNode1.nodeName}-${sourceNode1.nodeType}-${sourceNode1.nodeId}`)
    ).within(() => {
      cy.get(dataCy(`${sourceNode1.nodeName}-preview-data-btn`)).click();
    });
    cy.contains(noDataMsg).should('be.visible');
    cy.get(closeButton).click();
  });

  it('Should show preview data with table view by default for sink', () => {
    cy.window().then((window) => {
      skipPreviewTests = window.CDAP_CONFIG.hydrator.previewEnabled !== true;
    });
    if (skipPreviewTests) {
      skip();
    }
    // Start and run preview
    cy.get(dataCy('run-preview-btn')).click();
    cy.get(dataCy('stop-preview-btn')).should('be.visible');
    cy.get(dataCy('run-preview-btn'), { timeout: 70000 }).should('exist');
    cy.get(dataCy(`plugin-node-TPFSAvro-batchsink-2`)).within(() => {
      cy.get(dataCy(`${sinkNode.nodeName}-preview-data-btn`)).click();
    });
    // Should be able to toggle view and navigate records
    cy.get(dataCy('toggle-Table'), { timeout: 15000 }).should('exist');
    cy.contains('Charles,apples,10').should('be.visible');

    cy.get(dataCy('toggle-Table')).click();
    cy.get(dataCy('record-dropdown')).click();
    cy.contains('Record 3').click();
    cy.get(dataCy('fieldname-offset')).should('be.visible');
    cy.get(dataCy('previous-record-btn')).click();
    cy.contains(`Phyllis,beer,"10"`).should('be.visible');
    cy.get(closeButton).click();
  });

  it('Should show preview data for all inputs for joiner', () => {
    cy.window().then((window) => {
      skipPreviewTests = window.CDAP_CONFIG.hydrator.previewEnabled !== true;
    });
    if (skipPreviewTests) {
      skip();
    }

    cy.get(
      dataCy(`plugin-node-${joinerNode.nodeName}-${joinerNode.nodeType}-${joinerNode.nodeId}`)
    ).within(() => {
      cy.get(dataCy(`${joinerNode.nodeName}-preview-data-btn`)).click();
    });
    cy.contains(sourceNode1.nodeName).should('exist');
    cy.contains(sourceNode2.nodeName).should('exist');

    cy.get(dataCy('toggle-Table')).click();
    cy.get(dataCy(`tab-head-${sourceNode1.nodeName}`)).should('be.visible');
    cy.get(dataCy(`tab-head-${sourceNode2.nodeName}`)).should('be.visible');
    cy.contains(sourceNode1.nodeName).should('be.visible');

    cy.get(closeButton).click();
  });
});
