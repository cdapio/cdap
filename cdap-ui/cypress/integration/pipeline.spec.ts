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

import * as Helpers from '../helpers';
import { dataCy } from '../helpers';

const TEST_PIPELINE_NAME = '__UI_test_pipeline';
const TEST_PATH = '__UI_test_path';
const TEST_SENDER = '__UI_test_sender';
const TEST_RECIPIENT = '__UI_test@test.com';
const TEST_SUBJECT = '__UI_test_subject';
const PREVIEW_FAILED_BANNER_MSG = 'Please add a source and sink to the pipeline';

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

describe('Creating a pipeline', () => {
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
      cy.visit('/cdap', {
        onBeforeLoad: (win) => {
          win.sessionStorage.clear();
          win.sessionStorage.setItem('pipelineConfigTesting', 'true');
        },
      });
    });
  });

  beforeEach(() => {
    Helpers.getArtifactsPoll(headers);
  });

  afterEach(() => {
    // Delete the pipeline to clean up
    cy.cleanup_pipelines(headers, TEST_PIPELINE_NAME);
  });

  after(() => {
    cy.window().then((win) => {
      win.sessionStorage.removeItem('pipelineConfigTesting');
    });
  });

  it('can configure simple pipeline, including post-run actions', () => {
    // Go to Pipelines studio
    cy.visit('/cdap/ns/default/pipelines');
    cy.get('#resource-center-btn').click();
    cy.get('#create-pipeline-link').click();
    cy.url().should('include', '/studio');

    // Add an action node, to create minimal working pipeline
    cy.get('.item', {
      timeout: 10000,
    })
      .contains('Conditions and Actions')
      .click();
    cy.get('.item-body-wrapper')
      .contains('File Delete')
      .click();

    // Fill out required Path input field with some test value
    cy.get('.node-configure-btn')
      .invoke('show')
      .click();
    cy.get('[data-cy=configuration-group]')
      .contains('Path')
      .parent()
      .find('input')
      .click()
      .type(TEST_PATH);

    cy.get('[data-testid=close-config-popover]').click();

    // Click on Configure, toggle Instrumentation value, set up email alert, then Save
    cy.contains('Configure').click();
    cy.get('.label-with-toggle')
      .contains('Instrumentation')
      .parent()
      .as('instrumentationDiv');
    cy.get('@instrumentationDiv').contains('On');
    cy.get('@instrumentationDiv')
      .find('.toggle-switch')
      .click();
    cy.get('@instrumentationDiv').contains('Off');
    cy.contains('Pipeline alert').click();
    cy.contains('+').click();
    cy.contains('Send Email').click();
    // enter sender, recipients, subject, message

    cy.get('[data-cy="sender"]').within(() => {
      cy.get('input')
        .scrollIntoView()
        .focus()
        .type(TEST_SENDER);
    });
    cy.get('[data-cy="recipients"]').within(() => {
      cy.get('input').type(TEST_RECIPIENT);
    });
    cy.get('[data-cy="subject"]').within(() => {
      cy.get('input').type(TEST_SUBJECT);
    });
    // validate and see error
    cy.get('[data-cy="validate-btn"]').click();

    cy.contains('error found').should('exist');

    // Fix missing field to resolve error
    cy.get('[data-cy="message"]').within(() => {
      cy.get('button').click();
    });
    // validate
    cy.get('[data-cy="validate-btn"]').click();
    cy.wait(2000);
    cy.contains('error found').should('not.exist');

    // click next
    cy.get('[data-cy="next-btn"]').click();

    // click confirm
    cy.get('[data-cy="confirm-btn"]').click();

    // See email alert
    cy.get('[data-cy="saved-alerts"]').within(() => {
      cy.contains('Email').should('exist');
    });

    cy.get('[data-testid=config-apply-close]').click();
  });
  it('does not run Preview when user clicks No on postrun action modal', () => {
    cy.window().then((window) => {
      skipPreviewTests = window.CDAP_CONFIG.hydrator.previewEnabled !== true;
    });
    if (skipPreviewTests) {
      skip();
    }
    cy.get('[data-cy="pipeline-preview-btn"]').click();
    cy.get('[data-cy="preview-top-run-btn"]').click();
    cy.get('[data-cy="run-preview-action-modal"]').should('be.visible');

    // When user clicks No, Preview should not start
    cy.get('[data-cy="no-preview-btn"]').click();
    cy.get('[data-cy="preview-top-run-btn"]').should('be.visible');
  });
  it('runs Preview when user clicks Yes on postrun action modal', () => {
    cy.window().then((window) => {
      skipPreviewTests = window.CDAP_CONFIG.hydrator.previewEnabled !== true;
    });
    if (skipPreviewTests) {
      skip();
    }
    // When user clicks Yes, Preview should fail to start and show banner about missing source and sink
    cy.get('[data-cy="preview-top-run-btn"]').click();
    cy.get('[data-cy="run-preview-action-modal"]').should('be.visible');
    cy.get('[data-cy="yes-preview-btn"]').click();
    // Close banner reminding user to add source and sink
    cy.get('[data-cy="valium-banner-hydrator"]').contains(PREVIEW_FAILED_BANNER_MSG);
    cy.get('.close').click();
    cy.get('[data-cy="preview-active-btn"]')
      .should('be.visible')
      .click();
    cy.get('[data-cy="pipeline-schedule-modeless-btn"]').should('be.visible');
  });

  it('shows correct configuration after deploying pipeline', () => {
    // Name pipeline
    cy.get('.pipeline-name').click();
    cy.get('#pipeline-name-input')
      .type(TEST_PIPELINE_NAME)
      .type('{enter}');

    // Deploy pipeline
    cy.get('[data-testid=deploy-pipeline]').click();

    // Do assertions
    cy.url({ timeout: 60000 }).should('include', `/view/${TEST_PIPELINE_NAME}`);
    cy.contains(TEST_PIPELINE_NAME);
    cy.contains('FileDelete');
    cy.wait(5000);
    cy.get('.pipeline-configure-btn', { timeout: 60000 }).click();
    cy.get('[data-cy="tab-head-Pipeline config"]').click();
    cy.get('.label-with-toggle')
      .contains('Instrumentation')
      .parent()
      .as('instrumentationDiv');
    cy.get('@instrumentationDiv').contains('Off');
    cy.get('[data-testid=close-modeless]').click();
  });

  it.only(
      'opening pipeline with unknown workspace should still render the studio',
      () => {
        cy.visit(
            'pipelines/ns/default/studio?artifactType=cdap-data-pipeline&workspaceId=ebdbb6a7-8a8c-47b5-913f-9b75b1a0');
        cy.get(dataCy('app-navbar')).should('be.visible');
        cy.get(dataCy('navbar-toolbar')).should('be.visible');
        cy.get(dataCy('navbar-hamburger-icon')).click();
        cy.get(dataCy('navbar-home-link')).should('be.visible');
      })
});
