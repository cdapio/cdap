/*
 * Copyright © 2018 Cask Data, Inc.
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
const DUMMY_USERNAME = Cypress.env('username') || 'admin';
const DUMMY_PW = Cypress.env('password') || 'admin';
const INCORRECT_LOGIN = '__UI_test';
let isAuthEnabled = false;
describe('Logging in', () => {
  before(() => {
    cy.request({
      method: 'GET',
      failOnStatusCode: false,
      url: `http://${Cypress.env('host')}:11015/v3/namespaces`,
    }).then((response) => {
      // only login when ping request returns 401
      if (response.status === 401) {
        return cy.clearCookies();
        isAuthEnabled = true;
      }
    });
  });

  it("doesn't log user in when given incorrect credentials", () => {
    if (!isAuthEnabled) {
      cy.log('Effectively skipping test as auth is not enabled');
      return;
    }
    cy.visit('/login');
    cy.get('#username')
      .click()
      .type(INCORRECT_LOGIN);
    cy.get('#password')
      .click()
      .type(INCORRECT_LOGIN);
    cy.get('#submit').click();
    cy.url().should('include', '/login');
    cy.getCookie('CDAP_Auth_Token').should('not.exist');
  });

  it('logs user in when given correct credentials', () => {
    if (!isAuthEnabled) {
      cy.log('Effectively skipping test as auth is not enabled');
      return;
    }
    cy.visit('/login');
    cy.get('#username')
      .click()
      .type(DUMMY_USERNAME);
    cy.get('#password')
      .click()
      .type(DUMMY_PW);
    cy.get('#submit').click();
    cy.url({ timeout: 60000 }).should('include', '/cdap/ns');
    cy.getCookie('CDAP_Auth_Token').should('exist');
    cy.getCookie('CDAP_Auth_User').should('have.property', 'value', DUMMY_USERNAME);
  });
});
