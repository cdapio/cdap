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

import { dataCy, loginIfRequired, getArtifactsPoll } from '../helpers';

const PIPELINE_NAME = `postrunactions_test_${Date.now()}`;
const SENDER_EMAIL = 'sender@example.com';
const RECIPIENT_EMAIL = 'recipient@example.com';
const SUBJECT = 'Test Notification';
const MESSAGE = 'Your pipeline ran';
let headers = {};

describe('Pipeline post-run actions', () => {
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

  after(() => {
    // Delete the pipeline to clean up
    cy.cleanup_pipelines(headers, PIPELINE_NAME);
  });

  it('should set up an email notification that is available after deploy', () => {
    // Create a simple pipeline
    cy.visit('/pipelines/ns/default/studio');
    cy.url().should('include', '/studio');

    cy.upload_pipeline(
      'pipeline_with_macros.json',
      '#pipeline-import-config-link > input[type="file"]'
    );

    cy.wait(1000);

    cy.get('.pipeline-name').click();
    cy.get('#pipeline-name-input')
        .clear()
        .type(PIPELINE_NAME)
        .type('{enter}');
    cy.wait(1000);

    // Set up email notification
    cy.get(dataCy('pipeline-configure-modeless-btn')).click();
    cy.get(dataCy('pipeline-configure-alerts-tab')).click();
    cy.get(dataCy('post-run-alerts-create')).click();

    cy.wait(1000);
    cy.contains('Alerts').should('exist');

    cy.contains('Send Email').click();
    cy.wait(500);

    cy.get(`input${dataCy('sender')}`)
      .clear()
      .type(SENDER_EMAIL);

    cy.get(dataCy('recipients')).within(() => {
      cy.get(dataCy('key')).within(() => {
        cy.get('input')
          .clear()
          .type(RECIPIENT_EMAIL);
      });
    });

    cy.get(`input${dataCy('subject')}`).clear().type(SUBJECT);

    cy.window().then((win) => {
      cy.get('.ace-editor-ref').then((aceElement) => {
        const aceEditor = win.ace.edit(aceElement[0]);
        aceEditor.session.doc.setValue(MESSAGE);
      });
    });
    // Click out of ace editor
    cy.get(`input${dataCy('username')}`).click();

    cy.get(dataCy('next-btn')).click();
    cy.get(dataCy('confirm-btn')).click();

    cy.get('[data-testid="config-apply-close"]').click();

    // Deploy
    cy.get(dataCy('pipeline-draft-save-btn')).click();
    cy.get(dataCy('deploy-pipeline-btn')).click();
    cy.wait(10000);
    cy.get(dataCy('Deployed')).should('exist');

    // Verify that the settings made it to the deployed pipeline
    cy.get(dataCy('pipeline-configure-btn')).click();
    cy.get(dataCy('tab-head-Pipeline alert')).click();
    cy.get(dataCy('action-view')).click();
    cy.wait(1000);

    cy.get(`input${dataCy('sender')}`).should('have.value', SENDER_EMAIL);
    cy.get(dataCy('recipients')).within(() => {
      cy.get(dataCy('key')).within(() => {
        cy.get('input').should('have.value', RECIPIENT_EMAIL);
      });
    });
    cy.get(`input${dataCy('subject')}`).should('have.value', SUBJECT);
    cy.window().then((win) => {
      cy.get('.ace-editor-ref').then((aceElement) => {
        const aceEditor = win.ace.edit(aceElement[0]);
        const editorValue = aceEditor.session.doc.getValue();
        expect(editorValue).to.equal(MESSAGE);
      });
    });
  });
});
