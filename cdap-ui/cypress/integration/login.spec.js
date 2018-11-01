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
const DUMMY_USERNAME = 'alice';
const DUMMY_PW = 'alicepassword';
const INCORRECT_LOGIN = '__UI_test';

describe('Logging in', function() {
  it('logs user in when given correct credentials', function() {
    cy.visit('/');
    cy.get('#username')
      .click()
      .type(DUMMY_USERNAME);
    cy.get('#password')
      .click()
      .type(DUMMY_PW);
    cy.get('#submit').click();
    cy.url().should('include', '/cdap/ns/default');
    cy.getCookie('CDAP_Auth_Token').should('exist');
    cy.getCookie('CDAP_Auth_User').should('have.property', 'value', DUMMY_USERNAME);
  });

  it('doesn\t log user in when given incorrect credentials', function() {
    cy.visit('/');
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
});
