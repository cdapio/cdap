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
  it('Should rightly show jar and json file names', () => {
    cy.visit('/cdap/ns/default/pipelines');
    cy.get('#resource-center-btn').click();
    cy.get('button#upload-plugin').click();
    cy.fixture('example-transform-1.1.0-SNAPSHOT.jar').then((content) => {
      cy.get('[data-cy="plugin-jar-upload-container"]')
        .upload(content, 'example-transform-1.1.0-SNAPSHOT.jar', 'application/jar');
      cy.contains('example-transform-1.1.0-SNAPSHOT.jar');
    });
    cy.get('[data-cy="wizard-next-btn"]').click();
    cy.fixture('example-transform-1.1.0-SNAPSHOT.json').then((content) => {
      cy.get('[data-cy="plugin-json-upload-container"]')
        .upload(JSON.stringify(content, null, 2), 'example-transform-1.1.0-SNAPSHOT.json', 'application/json');
      cy.contains('example-transform-1.1.0-SNAPSHOT.json');
    });
  });

  it('Should throw valid error when trying to upload invalid files', () => {
    cy.visit('/cdap/ns/default/pipelines');
    cy.get('#resource-center-btn').click();
    cy.get('button#upload-plugin').click();
    cy.fixture('example-transform-1.1.0-SNAPSHOT.jar').then((content) => {
      cy.get('[data-cy="plugin-jar-upload-container"]')
        .upload(content, 'example-transform-1.1.0-SNAPSHOT.json', 'application/json');
    });
    cy.contains('Invalid plugin. Plugin must be a JAR file.');
    cy.get('[data-cy="wizard-next-btn"]').click();
    cy.fixture('example-transform-1.1.0-SNAPSHOT.jar').then((content) => {
      cy.get('[data-cy="plugin-json-upload-container"]')
        .upload(content, 'example-transform-1.1.0-SNAPSHOT.jar', 'application/jar');
    });
    cy.contains('Invalid plugin JSON. Plugin configuration should be in JSON format.');
    cy.get('[data-cy="wizard-finish-btn"]').should('have.attr', 'disabled', 'disabled');
  });
});
