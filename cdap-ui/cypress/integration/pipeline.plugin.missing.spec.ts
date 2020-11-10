/*
 * Copyright © 2018 Cask Data, Inc.
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

import{dataCy, loginIfRequired, getArtifactsPoll} from '../helpers';

let headers = {};


describe('Creating a pipeline with missing artifacts', () => {
  const pipelineName = `test-missing-artifact-${Date.now()}`; 
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
      cy.visit('/cdap', {
        onBeforeLoad: (win) => {
          win.sessionStorage.clear();
        },
      });
    });
  });

  beforeEach(() => {
    getArtifactsPoll(headers);
  });

  after(() => {
    cy.cleanup_pipelines(headers, pipelineName);
  });

  beforeEach(() => {
    cy.delete_artifact_via_api(headers, 'pdf-extractor-transform', '2.0.0');
  });

  it('should be rendered properly in the studio.', () => {
    cy.visit('pipelines/ns/default/studio');
    cy.upload_pipeline(
          'missing-artifact-pipeline.json',
          '#pipeline-import-config-link > input[type="file"]')
        .then((subject) => {
          expect(subject.length).to.be.eq(1);
          cy.get(dataCy('fix-all-btn')).click();
        });
    cy.get('[title="PDFExtractor"]').should('exist');
    cy.get(`[title="File"] ~ ${dataCy('node-properties-btn')}`)
        .should('not.be.disabled');
    cy.get(`[title="File"] ~ ${dataCy('node-properties-btn')}`).click({
      force: true
    });
    cy.get(`#File ${dataCy('node-badge')}`).should('does.not.exist');
    cy.get(dataCy('plugin-properties-validate-btn')).should('be.visible');
    cy.get('[data-testid=close-config-popover]').click();
    cy.get(dataCy('plugin-properties-validate-btn')).should('does.not.exist');
    cy.get(`[title="PDFExtractor"] ~ ${dataCy('node-properties-btn')}`)
        .should('be.disabled');
    cy.get(`#PDFExtractor ${dataCy('node-badge')}`).should('be.visible');
    cy.get(`#PDFExtractor ${dataCy('node-badge')}`).should('contain', '1');
    cy.get(`[title="PDFExtractor"] ~ ${dataCy('node-properties-btn')}`).click({
      force: true
    });
    cy.get(dataCy('plugin-properties-validate-btn')).should('does.not.exist');
    cy.get(`#PDFExtractor ${dataCy('node-badge')}`).click();
    cy.get(dataCy('plugin-properties-validate-btn')).should('does.not.exist');
    cy.get(dataCy('pipeline-metadata')).click({force: true});
    cy.get('#pipeline-name-input').should('be.visible');
    cy.get('#pipeline-name-input')
        .clear()
        .type(pipelineName)
        .type('{enter}');
    cy.get(dataCy('pipeline-draft-save-btn')).click();
    cy.get(dataCy('deploy-pipeline-btn')).click();
    cy.get(dataCy('valium-banner-hydrator')).should('be.visible');
    cy.get(dataCy('valium-banner-hydrator'))
        .should('contain', 'Artifact PDFExtractor is not available.');
    cy.get(dataCy('banner-dismiss-btn')).click();
    cy.get('#navbar-hub').click();
    cy.get('.search-input').type('Pdf');
    cy.get('.package-metadata-container').click();
    cy.get(dataCy('one_step_deploy_plugin-btn')).click();
    cy.get(dataCy('wizard-finish-btn')).click();
    cy.get(dataCy('wizard-result-icon-close-btn')).click();
    cy.get(dataCy('hub-close-btn')).click();
    cy.get(dataCy('pipeline-draft-save-btn')).click();
    cy.reload();
    cy.get('[title="PDFExtractor"]').should('exist');
    cy.get(`#PDFExtractor ${dataCy('node-badge')}`).should('does.not.exist');
    cy.get(`[title="PDFExtractor"] ~ ${dataCy('node-properties-btn')}`)
        .should('not.be.disabled');
    cy.get(`[title="PDFExtractor"] ~ ${dataCy('node-properties-btn')}`).click({
      force: true
    });
    cy.get(dataCy('plugin-properties-validate-btn')).should('be.visible');
    cy.close_node_property();
    cy.get(dataCy('deploy-pipeline-btn')).click();
    cy.get(dataCy('Deployed')).should('exist');
  });

  it('should be rendered properly in deployed view.', () => {
    cy.visit(`pipelines/ns/default/view/${pipelineName}`);
    cy.get('[title="PDFExtractor"]').should('exist');
    cy.get(`[title="File"] ~ ${dataCy('node-properties-btn')}`)
        .should('not.be.disabled');
    cy.get(`[title="PDFExtractor"] ~ ${dataCy('node-properties-btn')}`)
        .should('be.disabled');
  });
});
