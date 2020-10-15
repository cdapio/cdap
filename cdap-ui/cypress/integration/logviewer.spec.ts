/*
 * Copyright © 2020 Cask Data, Inc.
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

import { loginIfRequired, deployAndTestPipeline } from '../helpers';
import { dataCy } from '../helpers';

const LOG_VIEWER_TOGGLE = dataCy('log-viewer-btn-toggle');
const LOG_VIEWER_CONTAINER = dataCy('log-viewer');
const SCROLL_TO_LATEST = 'Scroll to Latest Logs';
const ADVANCED_LOGS = 'Advanced Logs';
const DOWNLOAD = 'Download All';
const LOG_VIEWER_CLOSE = dataCy('log-viewer-close-btn');
const LOG_LEVEL_TOGGLE = dataCy('log-viewer-log-level-toggle');
const LOG_LEVEL_POPOVER = dataCy('log-viewer-log-level-popover');
const LOG_VIEWER_CONTENT = dataCy('log-viewer-content');
const LOG_ROW = dataCy('log-viewer-row');
const LOG_MESSAGE = dataCy('log-message');

const logsPipeline = `logs_generator_${Date.now()}`;

const maybeDescribe = Cypress.env('cdap_environment') === 'cluster' ? describe.skip : describe;

maybeDescribe('LogViewer ', () => {
  // Uses API call to login instead of logging in manually through UI
  before(() => {
    loginIfRequired();
  });

  before(() => {
    deployAndTestPipeline('logs_generator-cdap-data-pipeline.json', logsPipeline, () => {
      cy.get(dataCy('pipeline-run-btn')).click();
      cy.get(dataCy('Running'), { timeout: 60000 });
    });
  });

  after(() => {
    cy.get('.pipeline-stop-btn').click({ force: true });
  });

  it('should show log viewer', () => {
    cy.get(LOG_VIEWER_TOGGLE).click();
    cy.get(LOG_VIEWER_CONTAINER).should('exist');
    cy.contains(SCROLL_TO_LATEST).should('exist');
    cy.contains(SCROLL_TO_LATEST).should('be.disabled');
    cy.contains(ADVANCED_LOGS).should('exist');
    cy.contains(DOWNLOAD).should('exist');
    cy.get(LOG_VIEWER_CLOSE).should('exist');
  });

  it('should have log level popover', () => {
    cy.get(LOG_LEVEL_POPOVER).should('exist');
    cy.get(LOG_LEVEL_TOGGLE).click();

    cy.get(LOG_LEVEL_POPOVER)
      .should('contain', 'ERROR')
      .and('contain', 'WARN')
      .and('contain', 'INFO')
      .and('contain', 'DEBUG')
      .and('contain', 'TRACE');
    cy.get(LOG_LEVEL_TOGGLE).click();
  });

  it('should default log level to INFO', () => {
    cy.get(LOG_LEVEL_TOGGLE).click();
    cy.get(dataCy('log-level-row-ERROR'))
      .find(dataCy('log-level-check'))
      .should('exist');
    cy.get(dataCy('log-level-row-WARN'))
      .find(dataCy('log-level-check'))
      .should('exist');
    cy.get(dataCy('log-level-row-INFO'))
      .find(dataCy('log-level-check'))
      .should('exist');
    cy.get(dataCy('log-level-row-DEBUG'))
      .find(dataCy('log-level-check'))
      .should('not.exist');
    cy.get(dataCy('log-level-row-TRACE'))
      .find(dataCy('log-level-check'))
      .should('not.exist');
    cy.get(LOG_LEVEL_TOGGLE).click();
  });

  it('should display logs', () => {
    cy.get(LOG_VIEWER_CONTENT)
      .contains('is started by user')
      .should('exist');
  });

  it('should be able to toggle Advanced Logs', () => {
    const warningMessage = 'This is a WARN';
    cy.get(LOG_VIEWER_CONTENT)
      .contains(warningMessage) // adding timeout to wait for application startup time
      .should('not.exist');
    cy.contains(ADVANCED_LOGS).click();
    cy.contains(`Hide ${ADVANCED_LOGS}`).should('exist');
    cy.get(LOG_VIEWER_CONTENT)
      .contains(warningMessage)
      .should('exist', { timeout: 180000 });
  });

  it('should be able to change log level', () => {
    cy.get(LOG_LEVEL_TOGGLE).click();
    cy.contains('TRACE').click();
    cy.get(LOG_LEVEL_POPOVER).should('not.be.visible');
    cy.get(LOG_VIEWER_CONTENT)
      .contains('DEBUG')
      .should('exist', { timeout: 60000 });
  });

  it('should stop poll on scroll up', () => {
    cy.get(LOG_VIEWER_CONTENT).scrollTo('center');
    cy.contains(SCROLL_TO_LATEST).should('be.enabled');
  });

  it('should fetch next logs when scroll to bottom', () => {
    let lastMessage;
    cy.get(LOG_ROW)
      .last()
      .find(LOG_MESSAGE)
      .invoke('text')
      .then((txt) => {
        lastMessage = txt;
      });

    cy.get(LOG_VIEWER_CONTENT).scrollTo('bottom');

    // wait for new logs to load
    cy.wait(5000);

    cy.get(LOG_ROW)
      .first()
      .find(LOG_MESSAGE)
      .invoke('text')
      .then((txt) => {
        expect(txt).not.equal(lastMessage);
      });

    cy.get(LOG_VIEWER_CONTENT).scrollTo('center');
  });

  it('should fetch previous logs when scroll to top', () => {
    cy.contains(SCROLL_TO_LATEST).click();
    cy.get(dataCy('loading-indicator')).should('exist');
    cy.get(dataCy('loading-indicator'), { timeout: 10000 }).should('not.exist');

    let firstMessage;
    cy.get(LOG_ROW)
      .first()
      .find(LOG_MESSAGE)
      .invoke('text')
      .then((txt) => {
        firstMessage = txt;
      });

    // scroll to top
    // simulating scroll behavior. The first scroll should stop the poll. The second scroll will fetch
    // previous logs.
    cy.get(LOG_VIEWER_CONTENT).scrollTo('center');
    cy.wait(300);
    cy.get(LOG_VIEWER_CONTENT).scrollTo(0, -100);

    // wait for new logs to load
    cy.wait(5000);

    cy.get(LOG_ROW)
      .first()
      .find(LOG_MESSAGE)
      .invoke('text')
      .then((txt) => {
        expect(txt).not.equal(firstMessage);
      });
    cy.get(LOG_VIEWER_CONTENT).scrollTo('center');
  });
});
