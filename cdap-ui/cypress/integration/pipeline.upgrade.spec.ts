/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import * as Helpers from '../helpers';
let headers = {};
describe('Pipeline Upgrade should work fine', () => {
  // Uses API call to login instead of logging in manually through UI
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
    const stub = cy.stub();
    cy.window().then((win) => {
      win.onbeforeunload = null;
    });
    cy.on('window:confirm', stub);
  });

  beforeEach(() => {
    Helpers.getArtifactsPoll(headers);
  });

  it('should not show upgrade modal on uploading pipeline with valid plugin versions', () => {
    // Go to Pipelines studio
    cy.visit('/cdap/ns/default/pipelines');
    cy.get('#resource-center-btn').click();
    cy.get('#create-pipeline-link').click();
    cy.url().should('include', '/studio');
    cy.upload_pipeline('pipeline1.json', '#pipeline-import-config-link > input[type="file"]').then(
      (subject) => {
        expect(subject.length).to.be.eq(1);
      }
    );
  });

  it('should show upgrade modal on uploading pipeline invalid plugin versions', () => {
    cy.visit('/cdap/ns/default/pipelines');
    cy.get('#resource-center-btn').click();
    cy.get('#create-pipeline-link').click();
    cy.url().should('include', '/studio');
    cy.upload_pipeline(
      'pipeline_old.json',
      '#pipeline-import-config-link > input[type="file"]'
    ).then((subject) => {
      expect(subject.length).to.be.eq(1);
      cy.get('.hydrator-modal.node-config-modal.upgrade-modal');
      cy.should('contain', 'Missing Plugin Artifacts');
      cy.contains('Find Plugin in Hub').click();
      cy.get('.cdap-modal.cask-market');
      cy.contains('Hub');
    });
  });
});
