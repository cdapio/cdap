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
const testPipeline = `test_pipeline_${Date.now()}`;

describe('Using control center to see and filter things', () => {
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
  before((done) => {
    // Deploy a pipeline to have a pipeline and some datasets to see
    Helpers.deployAndTestPipeline('fll_airport_pipeline2.json', testPipeline, done);
  });
  after(() => {
    // Delete the pipeline to clean up
    cy.cleanup_pipelines(headers, testPipeline);
  });
  it('allows users to open filter menu and filter out pipelines', () => {
    cy.visit('/cdap/ns/default/control');
    // TO DO: figure out how to replace this cy.wait with a timeout
    cy.wait(2000);
    cy.get(`.entities-all-list-container [data-cy="${testPipeline}-header"]`).should(
      'have.length',
      1
    );
    // find filter dropdown and unselect "applications"

    // TO DO: Investigate why cy.click() doesn't work here.
    // cy.get('[data-cy="filter-dropdown"]').click();
    cy.get('[data-cy="filter-dropdown"]').then((dropdown) => {
      dropdown.click();
    });
    // cy.get('[data-cy="Applications-input"]').click();
    cy.get('[data-cy="Applications-input"]').then((checkbox) => {
      checkbox.click();
    });

    cy.get('.text-center > .fa', {
      timeout: 2000,
    }).should('not.exist');
    cy.get(`.entities-all-list-container [data-cy="${testPipeline}-header"]`).should('not.exist');
  });
});
