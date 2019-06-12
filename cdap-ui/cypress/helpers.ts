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

const username = Cypress.env('username') || 'admin';
const password = Cypress.env('password') || 'admin';
let isAuthEnabled = false;
let authToken = null;

function loginIfRequired() {
  if (isAuthEnabled && authToken !== null) {
    cy.setCookie('CDAP_Auth_Token', authToken);
    cy.setCookie('CDAP_Auth_User', username);
    Cypress.Cookies.defaults({
      whitelist: ['CDAP_Auth_Token', 'CDAP_Auth_User'],
    });
    return;
  }
  return cy
    .request({
      method: 'GET',
      url: `http://${Cypress.env('host')}:11015/v3/namespaces`,
      failOnStatusCode: false,
    })
    .then((response) => {
      // only login when ping request returns 401
      if (response.status === 401) {
        isAuthEnabled = true;
        cy.request({
          method: 'POST',
          url: '/login',
          headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
          body: JSON.stringify({
            username,
            password,
          }),
        }).then((res) => {
          const respBody = JSON.parse(res.body);
          authToken = respBody.access_token;
          cy.setCookie('CDAP_Auth_Token', respBody.access_token);
          cy.setCookie('CDAP_Auth_User', username);
          Cypress.Cookies.defaults({
            whitelist: ['CDAP_Auth_Token', 'CDAP_Auth_User'],
          });
        });
      }
    });
}

function getArtifactsPoll(headers, retries = 0) {
  if (retries === 3) {
    return;
  }
  cy.request({
    method: 'GET',
    url: `http://${Cypress.env('host')}:11015/v3/namespaces/default/artifacts?scope=SYSTEM`,
    failOnStatusCode: false,
    headers,
  }).then((response) => {
    if (response.status >= 400) {
      return getArtifactsPoll(headers, retries + 1);
    }
    return;
  });
}

export { loginIfRequired, getArtifactsPoll };
