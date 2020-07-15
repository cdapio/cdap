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

import * as Helpers from '../helpers';

let headers = {};
const FAKE_NAMESPACE = 'fakeNamespace';
const SELECTOR_404_MSG = Helpers.dataCy('page-404-error-msg');
const SELECTOR_404_DEFAULT_MSG = Helpers.dataCy('page-404-default-msg');

describe('Page level error because of ', () => {
  // Uses API call to login instead of logging in manually through UI
  before(() => {
    Helpers.loginIfRequired().then(() => {
      cy.getCookie('CDAP_Auth_Token')
        .then((cookie) => {
          if (!cookie) {
            return;
          }
          headers = {
            Authorization: 'Bearer ' + cookie.value,
          };
        })
        .then(Helpers.getSessionToken)
        .then(
          (sessionToken) =>
            (headers = Object.assign({}, headers, { 'Session-Token': sessionToken }))
        )
        .then(() => {
          cy.start_wrangler(headers);
        });
    });
  });

  beforeEach(() => {
    Helpers.getArtifactsPoll(headers);
  });

  it('no namespace in home page should show 404', () => {
    // Go to home page
    cy.visit(`/cdap/ns/${FAKE_NAMESPACE}`);
    cy.get(SELECTOR_404_MSG).should('exist');
  });

  it('no namespace in pipeline studio page should show 404', () => {
    // Go to Pipelines studio
    cy.visit(`/pipelines/ns/${FAKE_NAMESPACE}/studio`);
    cy.get(SELECTOR_404_MSG).should('exist');
  });

  it('no namespace in pipeline list page should show 404', () => {
    // Need to turn off automatic test failure due to unhandled error handling. 
    // (CDAP-16414) We throw an error in DeployedPipelineView when there are graphQL errors. The errors are caught by Error Boundary. 
    cy.on('uncaught:exception', (err, runnable) => {
      return false;
    })
    // Go to Pipelines list
    cy.visit(`/cdap/ns/${FAKE_NAMESPACE}/pipelines`);
    cy.get(SELECTOR_404_MSG).should('exist');
  });

  it('no namespace in pipeline detail page should show 404', () => {
    // Go to Pipeline details page
    cy.visit(`/cdap/ns/${FAKE_NAMESPACE}/view/pipelineName`);
    cy.get(SELECTOR_404_MSG).should('exist');
  });

  it('no namespace in pipeline drafts page should show 404', () => {
    // Go to Pipelines drafts
    cy.visit(`/cdap/ns/${FAKE_NAMESPACE}/pipelines/drafts`);
    cy.get(SELECTOR_404_MSG).should('exist');
  });

  it('no namespace in wrangler should show 404', () => {
    // Go to wrangler
    cy.visit(`/cdap/ns/${FAKE_NAMESPACE}/wrangler`);
    cy.get(SELECTOR_404_MSG).should('exist');
  });

  it('no workspace in wrangler should show 404', () => {
    // Go to wrangler workspace
    cy.visit('/cdap/ns/default/wrangler/invalid-workspace-id');
    cy.get(SELECTOR_404_MSG).should('exist');
  });

  it('no namespace in metadata page should show 404', () => {
    // Go to metadata page
    cy.visit(`/metadata/ns/${FAKE_NAMESPACE}`);
    cy.get(SELECTOR_404_MSG).should('exist');
  });

  it('no namespace in metadata search results page should show 404', () => {
    // Go to metadata search results page
    cy.visit(`/metadata/ns/${FAKE_NAMESPACE}/search/search_term/result`);
    cy.get(SELECTOR_404_MSG).should('exist');
  });

  it('no valid path should show 404 in pipeline studio', () => {
    // Go to pipeline studio page
    cy.visit(`/pipelines/ns/default/studioInvalidPath`);
    cy.get(SELECTOR_404_DEFAULT_MSG).should('exist');
  });

  it('no valid path should show 404 in pipeline details', () => {
    // Go to pipeline details page
    cy.visit(`/pipelines/ns/default/viewInvalidPipelineDetails/pipelineName`);
    cy.get(SELECTOR_404_DEFAULT_MSG).should('exist');
  });

  it('no valid pipeline should show 404 in pipeline details', () => {
    // Go to pipeline details page of invalid pipeline
    cy.visit(`/pipelines/ns/default/view/invalidPipelineName`);
    cy.get(SELECTOR_404_MSG).should('exist');
  });

  it('no valid path should show 404 in metadata page', () => {
    // Go to metadata search results page
    cy.visit(`/metadata/ns/default/search/search_term/resultinvalidPath`);
    cy.get(SELECTOR_404_DEFAULT_MSG).should('exist');
  });

  it('no valid path should show 404 in wrangler', () => {
    // Go to wrangler
    cy.visit('/cdap/ns/default/wranglerInvalidPath/invalid-workspace-id');
    cy.get(SELECTOR_404_DEFAULT_MSG).should('exist');
  });

  it('no valid path from node server should show 404', () => {
    // Go to any random invalid path
    cy.visit('/randomInvalidPath');
    cy.get(SELECTOR_404_DEFAULT_MSG).should('exist');
  });
});
