/*
 * Copyright © 2021 Cask Data, Inc.
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

const { dataCy } = Helpers;

function deleteProfile(path) {
  cy.request({
    method: 'POST',
    url: `http://${Cypress.env('host')}:11015/${path}/disable`,
    headers,
  }).then((disableRes) => {
    expect(disableRes.status).to.be.eq(200);

    cy.request({
      method: 'DELETE',
      url: `http://${Cypress.env('host')}:11015/${path}`,
      headers,
    }).then((deleteRes) => {
      expect(deleteRes.status).to.be.eq(200);
    });
  });
}

describe('Compute Profile', () => {
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
  });

  // Due to special handling of system namespace, we need to explicitly test navigating into system namespace
  // profile pages
  it('should be able to create system compute profile', () => {
    cy.visit('cdap/ns/system/profiles/create');
    cy.get(dataCy('provisioner-gcp-dataproc')).should('exist');
    cy.get(dataCy('provisioner-gcp-dataproc')).click();

    cy.get(dataCy('profile-create-btn')).should('be.disabled');

    const profileName = 'test-system-compute';
    cy.get(dataCy('profileLabel')).type(profileName);
    cy.get(dataCy('profileName'))
      .invoke('val')
      .then((text) => {
        expect(text).equals(profileName);
      });

    // ace editor textarea element is covered by another element, so have to force the type to true
    cy.get(dataCy('profileDescription')).type('system profile for integration test', {
      force: true,
    });

    cy.get(dataCy('projectId')).type('test');
    cy.get(dataCy('accountKey')).type('test');
    cy.get(dataCy('profile-create-btn')).click();

    cy.get(dataCy(`profile-list-${profileName}`), { timeout: 15000 }).should('exist');
    deleteProfile(`v3/profiles/${profileName}`);
  });

  it('should be able to navigate to namespace compute profile create', () => {
    cy.visit('cdap/ns/default/profiles/create');
    cy.get(dataCy('provisioner-gcp-dataproc')).should('exist');
    cy.get(dataCy('provisioner-gcp-dataproc')).click();

    cy.get(dataCy('profile-create-btn')).should('be.disabled');

    const profileName = 'test-namespace-compute';
    cy.get(dataCy('profileLabel')).type(profileName);
    cy.get(dataCy('profileName'))
      .invoke('val')
      .then((text) => {
        expect(text).equals(profileName);
      });

    // ace editor textarea element is covered by another element, so have to force the type to true
    cy.get(dataCy('profileDescription')).type('namespace profile for integration test', {
      force: true,
    });

    cy.get(dataCy('projectId')).type('test');
    cy.get(dataCy('accountKey')).type('test');
    cy.get(dataCy('profile-create-btn')).click();

    cy.get(dataCy(`profile-list-${profileName}`), { timeout: 15000 }).should('exist');

    // cleanup
    deleteProfile(`v3/namespaces/default/profiles/${profileName}`);
  });
});
