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

import { loginIfRequired, dataCy, generateDraftFromPipeline } from '../helpers';
import { parse } from 'query-string';

let headers = {};
const dummyproject = 'dummyproject';
const dummydataset = 'dummydataset';

describe('Pipeline Drafts tests', () => {
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
  it('Should save/edit a pipeline draft successfully', () => {
    const TEST_PIPELINE_NAME = `Test_Draft_Pipeline_${Date.now()}`;
    cy.visit('/pipelines/ns/default/studio');
    cy.create_simple_pipeline().then(({ sourceNodeId }) => {
      cy.open_node_property(sourceNodeId);
      cy.get('input[data-cy="datasetProject"]').type(dummyproject);
      cy.get('input[data-cy="dataset"]').type(dummydataset);
      cy.close_node_property();
      cy.get('.pipeline-name').click();
      cy.get('#pipeline-name-input')
        .type(TEST_PIPELINE_NAME)
        .type('{enter}');
      cy.get('body').contains(`${TEST_PIPELINE_NAME} saved successfully`);
      cy.url({ timeout: 60000 }).should('include', 'draftId');
      cy.reload().then(() => {
        cy.get_pipeline_json()
          .then((pipelinejson) => {
            const sourceNode = pipelinejson.config.stages.find(
              (node) => node.plugin.type === 'batchsource' && node.plugin.name === 'BigQueryTable'
            );
            const sourceProperties = sourceNode.plugin.properties;
            expect(sourceProperties.datasetProject).to.equal(dummyproject);
            expect(sourceProperties.dataset).to.equal(dummydataset);

            cy.open_node_property(sourceNodeId);
            cy.get('input[data-cy="datasetProject"]')
              .clear()
              .type(`new_${dummyproject}`);
            cy.get('input[data-cy="dataset"]')
              .clear()
              .type(`new_${dummydataset}`);
            cy.close_node_property();
            cy.get('[data-cy="pipeline-draft-save-btn"]').click();
          })
          .then(() => {
            cy.reload().then(() => {
              cy.get_pipeline_json().then((pipelinejson) => {
                const sourceNode = pipelinejson.config.stages.find(
                  (node) =>
                    node.plugin.type === 'batchsource' && node.plugin.name === 'BigQueryTable'
                );
                const sourceProperties = sourceNode.plugin.properties;
                expect(sourceProperties.datasetProject).to.equal(`new_${dummyproject}`);
                expect(sourceProperties.dataset).to.equal(`new_${dummydataset}`);
              });
            });
          });
      });
    });
  });

  it('Should delete draft upon publishing', () => {
    const TEST_PIPELINE_NAME = `Test_Draft_Pipeline_${Date.now()}`;
    cy.visit('/pipelines/ns/default/studio');
    cy.upload_pipeline(
      'fll_wrangler-test-pipeline.json',
      '#pipeline-import-config-link > input[type="file"]'
    );
    cy.wait(10000);
    cy.get('.pipeline-name').click();
    cy.get('#pipeline-name-input')
      .clear()
      .type(`pre_publish_${TEST_PIPELINE_NAME}`)
      .type('{enter}');
    cy.get('body').contains(`pre_publish_${TEST_PIPELINE_NAME} saved successfully`);
    cy.url({ timeout: 60000 }).should('include', 'draftId');
    cy.visit('/cdap/ns/default/pipelines/drafts');
    cy.get('[data-cy="draft-pipeline-table"] .grid.grid-container .grid-header')
      .contains('Last saved')
      .click()
      .click();
    cy.contains(`pre_publish_${TEST_PIPELINE_NAME}`).click();
    cy.get(dataCy('deploy-pipeline-btn')).click();
    cy.url({ timeout: 60000 }).should('include', `pre_publish_${TEST_PIPELINE_NAME}`);
    cy.visit('/cdap/ns/default/pipelines/drafts');
    cy.get('[data-cy="draft-Old_TestPipeline1_migrated"]').should('not.exist');
  });

  it('Should upgrade draft using old api to new draft api while editing old drafts', () => {
    cy.visit('/pipelines/ns/default/studio');
    const newPipelineDraftName = `NewTestPipeline2_${Date.now()}`;
    generateDraftFromPipeline('old_userstore_draft.json').then(
      ({ pipelineDraft, pipelineName }) => {
        cy.upload_draft_via_api(headers, pipelineDraft);
        cy.visit('/cdap/ns/default/pipelines/drafts');
        cy.get('[data-cy="draft-pipeline-table"] .grid.grid-container .grid-header')
          .contains('Last saved')
          .click()
          .click();
        cy.get('body')
          .contains(pipelineName)
          .click();
        cy.url({ timeout: 60000 }).should('include', 'draftId');
        cy.location('search').then((searchStr) => {
          const queryObject = parse(searchStr);
          const queryId = queryObject.queryId;
          cy.get('.pipeline-name').click();
          cy.get('#pipeline-name-input')
            .clear()
            .type(newPipelineDraftName)
            .type('{enter}');
          cy.get('body').contains(`Draft ${newPipelineDraftName} saved successfully.`);
          cy.visit('/cdap/ns/default/pipelines/drafts');
          cy.get('body').contains(newPipelineDraftName);
          cy.request({
            failOnStatusCode: false,
            headers,
            url: `http://${Cypress.env(
              'host'
            )}:11015/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/default/drafts/${queryId}`,
          }).then((response) => {
            expect(response.status).to.equal(404);
          });
        });
      }
    );
  });

  it('Should delete drafts using either old or the new API', () => {
    const TEST_PIPELINE_NAME = `Test_Draft_Pipeline_${Date.now()}`;
    cy.visit('/pipelines/ns/default/studio');
    cy.create_simple_pipeline().then(() => {
      cy.get('.pipeline-name').click();
      cy.get('#pipeline-name-input')
        .type(TEST_PIPELINE_NAME)
        .type('{enter}');
      cy.get('body').contains(`${TEST_PIPELINE_NAME} saved successfully`);
      cy.url({ timeout: 60000 }).should('include', 'draftId');
      cy.visit('/cdap/ns/default/pipelines/drafts');
      cy.get('[data-cy="draft-pipeline-table"] .grid.grid-container .grid-header')
        .contains('Last saved')
        .click()
        .click();
      cy.get('body').contains(TEST_PIPELINE_NAME);
      cy.get(
        `[data-cy="draft-${TEST_PIPELINE_NAME}"] .action .actions-popover > div[tag="span"]`
      ).click();
      cy.get(`[data-cy="draft-${TEST_PIPELINE_NAME}"] .action .actions-popover`)
        .contains('Delete')
        .then((el) => el.trigger('click'));
      cy.get('[data-cy="confirm-dialog"] button[data-cy="Delete"]').click();
      cy.get('[data-cy="confirm-dialog"]').should('not.exist');
      cy.visit('/cdap/ns/default/pipelines/drafts');
      cy.get('[data-cy="draft-pipeline-table"] .grid.grid-container .grid-header')
        .contains('Last saved')
        .click()
        .click();
      cy.get(`[data-cy="draft-${TEST_PIPELINE_NAME}"]`).should('not.exist');
    });
    generateDraftFromPipeline('old_userstore_draft.json').then(
      ({ pipelineDraft, pipelineName }) => {
        cy.upload_draft_via_api(headers, pipelineDraft);
        cy.visit('/cdap/ns/default/pipelines/drafts');
        cy.get('[data-cy="draft-pipeline-table"] .grid.grid-container .grid-header')
          .contains('Last saved')
          .click()
          .click();
        cy.get(`[data-cy="draft-${pipelineName}"]`).should('exist');
        cy.get(
          `[data-cy="draft-${pipelineName}"] .action .actions-popover > div[tag="span"]`
        ).click();
        cy.get(`[data-cy="draft-${pipelineName}"] .action .actions-popover`)
          .contains('Delete')
          .then((el) => el.trigger('click'));
        cy.get('[data-cy="confirm-dialog"] button[data-cy="Delete"]').click();
        cy.get('[data-cy="confirm-dialog"]').should('not.exist');
        cy.get('[data-cy="draft-pipeline-table"] .grid.grid-container .grid-header')
          .contains('Last saved')
          .click()
          .click();
        cy.get(`[data-cy="draft-${pipelineName}"]`).should('not.exist');
      }
    );
  });
});
