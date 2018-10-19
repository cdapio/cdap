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

const DUMMY_USERNAME = 'alice';
const DUMMY_PW = 'alicepassword';

function loginIfRequired() {
  cy.visit('/');
  cy.request({
    method: 'GET',
    url: `http://${Cypress.env('host')}:11015/v3/namespaces`,
    failOnStatusCode: false,
  }).then((response) => {
    // only login when ping request returns 401
    if (response.status === 401) {
      cy.request({
        method: 'POST',
        url: '/login',
        headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
        body: JSON.stringify({
          username: DUMMY_USERNAME,
          password: DUMMY_PW,
        }),
      }).then((response) => {
        expect(response.status).to.be.at.least(200);
        expect(response.status).to.be.lessThan(300);
        const respBody = JSON.parse(response.body);
        cy.setCookie('CDAP_Auth_Token', respBody.access_token);
        cy.setCookie('CDAP_Auth_User', DUMMY_USERNAME);
        cy.visit('/', {
          onBeforeLoad: (win) => {
            win.sessionStorage.setItem('showWelcome', 'false');
          },
        });
        cy.url().should('include', '/cdap/ns/default');
        cy.getCookie('CDAP_Auth_Token').should('exist');
        cy.getCookie('CDAP_Auth_User').should('have.property', 'value', DUMMY_USERNAME);
      });
    }
  });
}

function getAuthHeaders() {
  let authTokenCookie = cy.getCookie('CDAP_Auth_Token');
  let headers = null;
  if (authTokenCookie) {
    Cypress.Cookies.preserveOnce('CDAP_Auth_Token');
    headers = {
      Authorization: 'Bearer ' + authTokenCookie.value,
    };
  }
  return headers;
}

export { loginIfRequired, getAuthHeaders };
