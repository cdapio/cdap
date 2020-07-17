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

import * as Helpers from '../helpers';

const nullSplitterPipeline = `null_splitter_pipeline_${Date.now()}`;
const unionConditionPipeline = `union_condition_splitter_${Date.now()}`;
const pipelines = [nullSplitterPipeline, unionConditionPipeline];
const conditionPreviewMsg = 'Preview data is not supported for condition stages.';
const closeButton = '[data-testid="close-config-popover"]';
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

describe('Pipelines with plugins having more than one endpoints', () => {
  before(() => {
    Helpers.loginIfRequired().then(() => {
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

  after(() => {
    // Delete the pipeline to clean up
    pipelines.forEach((pipeline) => cy.cleanup_pipelines(headers, pipeline));
  });

  it('Should upload pipeline with union splitter and condition plugins', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.get('#resource-center-btn').click();
    cy.get('#create-pipeline-link').click();
    cy.url().should('include', '/studio');
    cy.upload_pipeline(
      'union_condition_splitter_pipeline_v1-cdap-data-pipeline.json',
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
      .type(unionConditionPipeline)
      .type('{enter}');
  });
  it('Should show preview data for multiple outputs for splitter and correct message for conditional', () => {
    cy.window().then((window) => {
      skipPreviewTests = window.CDAP_CONFIG.hydrator.previewEnabled !== true;
    });
    if (skipPreviewTests) {
      skip();
    }
    cy.get(Helpers.dataCy('pipeline-preview-btn')).click();
    cy.get(Helpers.dataCy('preview-top-run-btn')).click();
    cy.get(Helpers.dataCy('stop-preview-btn')).should('be.visible');
    cy.get(Helpers.dataCy('preview-top-run-btn'), { timeout: 60000 }).should('exist');

    // Check number of output fields for null splitter
    cy.get(Helpers.dataCy('UnionSplitter-preview-data-btn')).click();
    cy.get(Helpers.dataCy('toggle-Table')).should('exist');
    cy.get('h3')
      .contains('Int')
      .should('be.visible');
    cy.get('h3')
      .contains('String')
      .should('be.visible');
    cy.get(closeButton).click();
    // Check messaging for condition plugin
    cy.get(`${Helpers.dataCy('plugin-node-Conditional-condition-5')} .node .node-configure-btn`)
      .invoke('show')
      .click({ force: true });
    cy.contains('Preview').click();
    cy.get('h3')
      .contains(conditionPreviewMsg)
      .should('exist');
    cy.get(closeButton).click();
    cy.get(Helpers.dataCy('preview-active-btn')).click();
  });

  it('Should deploy pipeline with union splitter and condition plugins', (done) => {
    cy.get('[data-testid=deploy-pipeline]').click();
    cy.get('[data-cy="Deployed"]', { timeout: 60000 }).should('contain', 'Deployed');
    cy.url()
      .should('include', `/view/${unionConditionPipeline}`)
      .then(() => done());
  });

  it('Should upload and deploy pipeline with null splitter plugin', (done) => {
    Helpers.deployAndTestPipeline(
      'null_splitter_pipeline-cdap-data-pipeline.json',
      nullSplitterPipeline,
      done
    );
  });
});
