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

import { dataCy, loginIfRequired } from '../helpers';

let headers = {};

const TEST_KEY = 'name';
const TEST_VALUE = 'hello';

describe('Setting and saving preferences', () => {
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

  it('Should show error message if user tries to set profile at the instance level', () => {
    cy.visit('cdap/administration/configuration');

    cy.get(dataCy('system-prefs-accordion')).click();
    cy.get(dataCy('edit-system-prefs-btn')).click();
    cy.get(dataCy('key-value-pair-0')).within(() => {
      cy.get('input[placeholder="key"]')
        .clear()
        .type('system.profile.name');
      cy.get('input[placeholder="value"]')
        .clear()
        .type(TEST_VALUE);
    });
    cy.get(dataCy('save-prefs-btn')).click();
    cy.get('.preferences-error').should('exist');
  });
  it('Should allow user to save valid preference at instance level after fixing error', () => {
    cy.get(dataCy('key-value-pair-0')).within(() => {
      cy.get('input[placeholder="key"]')
        .clear()
        .type(TEST_KEY);
    });
    cy.get(dataCy('save-prefs-btn')).click();
    cy.get('.grid-row').within(() => {
      cy.contains(TEST_KEY).should('be.visible');
      cy.contains(TEST_VALUE).should('be.visible');
    });
  });
});
