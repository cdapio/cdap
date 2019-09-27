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

let headers = {};
const fllPipeline = `fll_pipeline_${Date.now()}`;

describe('Generating and navigating field level lineage for datasets', () => {
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
  before(() => {
    // run a pipeline to generate lineage
    Helpers.deployAndTestPipeline('fll_airport_pipeline2.json', fllPipeline, () => {
      cy.get('[data-cy="pipeline-run-btn"]').click();
      cy.get('[data-cy="Succeeded"]', { timeout: 360000 }).should('contain', 'Succeeded');
    });
  });
  after(() => {
    // Delete the pipeline to clean up
    cy.cleanup_pipelines(headers, fllPipeline);
  });
  it('Should show lineage for the default time frame (last 7 days)', () => {
    cy.visit('cdap/ns/default/datasets/Airport_sink/fll-experiment');
    // should see last 7 days of lineage selected by default
    cy.get('[data-cy="fll-time-picker"]').should(($div) => {
      expect($div).to.contain('Last 7 days');
    });
    // should see the correct fields for the selected dataset
    cy.get('[data-cy="target-fields"] .grid-row').within(($fields) => {
      // should see only 'body' field for the impact dataset, assuming no previous lineage exists
      expect($fields).to.contain('body');
    });
  });
  it('Should show operations for target field', () => {
    // focus on a field with outgoing operations
    cy.get('[data-cy="target-fields"]').within(() => {
      cy.contains('body').click();
      cy.get('[data-cy="fll-view-dropdown"]').click();
    });
    cy.get('[data-cy="fll-view-incoming"]').click();
    cy.get('.operations-container').should('exist');
    cy.get('.modal-title .close-section').click();
  });
  it('Should allow user to see field level lineage for a custom date range', () => {
    // click on date picker dropdown and choose custom date range

    cy.get('[data-cy="time-picker-dropdown"]').click();
    cy.get('[data-cy="CUSTOM"]').click();
    cy.get('[data-cy="time-range-selector"]').should('exist');
    cy.get('[data-cy="time-range-selector"]').within(() => {
      cy.contains('Start Time').click();
    });
    cy.get('.react-calendar').within(() => {
      // start of range is two years and one month ago, and first available day
      // Go back two years
      cy.get('.react-calendar__navigation__prev2-button').click();
      cy.get('.react-calendar__navigation__prev2-button').click();
      // Go back one month
      cy.get('.react-calendar__navigation__prev-button').click();
    });
    // Choose first available day of the month
    cy.get('.react-calendar__month-view__days').within(() => {
      cy.get('button:enabled')
        .first()
        .click();
    });

    cy.get('[data-cy="time-range-selector"]').within(() => {
      cy.contains('End Time').click();
    });
    // end of range: two years and zero months for now, first available day
    cy.get('.react-calendar').within(() => {
      // Go forward one month from the start date
      cy.get('.react-calendar__navigation__next-button').click();
    });
    // Choose first available day of that month
    cy.get('.react-calendar__month-view__days').within(() => {
      cy.get('button:enabled')
        .first()
        .click();
    });

    cy.get('.done-button')
      .contains('Done')
      .click();

    // Should see no fields with operations since there is no lineage for the date range
    cy.get('[data-cy="target-fields"] .grid-row')
      .first()
      .click();
    cy.get('[data-cy="fll-view-dropdown"]').click();
    cy.get('[data-cy="fll-view-incoming"]').should('have.class', 'Mui-disabled');
    cy.get('[data-cy="fll-view-outgoing"]').should('have.class', 'Mui-disabled');
  });
});
