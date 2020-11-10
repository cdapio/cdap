/*
 * Copyright © 2019 Cask Data, Inc.
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
import { ConnectionType } from '../../app/cdap/components/DataPrepConnections/ConnectionType';
import {
  DEFAULT_GCS_FILE,
  DEFAULT_GCS_FOLDER,
  DEFAULT_GCS_CONNECTION_NAME,
} from '../support/constants';

let headers;

describe('Wrangler GCS tests', () => {
  before(() => {
    return Helpers.loginIfRequired()
      .then(() => {
        cy.getCookie('CDAP_Auth_Token').then((cookie) => {
          if (!cookie) {
            return cy.wrap(headers);
          }
          headers = {
            Authorization: 'Bearer ' + cookie.value,
          };
        });
      })
      .then(Helpers.getSessionToken)
      .then(
        (sessionToken) => (headers = Object.assign({}, headers, { 'Session-Token': sessionToken }))
      )
      .then(() => cy.start_wrangler(headers));
  });

  it('Should successfully test GCS connection', () => {
    cy.test_GCS_connection(DEFAULT_GCS_CONNECTION_NAME);
    cy.get('.card-action-feedback.SUCCESS');
    cy.contains('Test connection successful');
  });

  it('Should show appropriate message when test connection fails', () => {
    cy.test_GCS_connection('unknown_gcs_connection', 'unknown_project', 'unknown_path');
    cy.get('.card-action-feedback.DANGER');
    cy.contains('Test connection failed');
  });

  it('Should create GCS connection', () => {
    cy.create_GCS_connection(DEFAULT_GCS_CONNECTION_NAME);
    cy.get(`[data-cy="wrangler-${ConnectionType.GCS}-connection-${DEFAULT_GCS_CONNECTION_NAME}"]`);
  });

  it('Should show proper error message when trying to create existing connection', () => {
    cy.create_GCS_connection(DEFAULT_GCS_CONNECTION_NAME);
    cy.get('.card-action-feedback.DANGER');
    cy.get('.card-action-feedback.DANGER .main-message .expand-icon').click();
    cy.get('.card-action-feedback.DANGER .stack-trace').should(
      'contain',
      `'${DEFAULT_GCS_CONNECTION_NAME}' already exists.`
    );
  });

  it('Should be able navigate inside GCS connection & create workspace', () => {
    cy.visit('/cdap/ns/default/connections');
    cy.get(
      `[data-cy="wrangler-${ConnectionType.GCS}-connection-${DEFAULT_GCS_CONNECTION_NAME}"]`
    ).click();
    cy.get(Helpers.dataCy('gcs-search-box')).type(DEFAULT_GCS_FOLDER);
    cy.contains(DEFAULT_GCS_FOLDER).click();
    cy.contains(DEFAULT_GCS_FILE).click();
    cy.url().should('contain', '/ns/default/wrangler');
  });

  it('Should show appropriate error when navigating to incorrect GCS connection', () => {
    const connName = 'gcs_unknown_connection';
    cy.visit(`/cdap/ns/default/connections/gcs/${connName}`);
    cy.contains(`Connection '${connName}' does not exist`);
    cy.contains('No files or directories found in this bucket');
  });

  it('Should delete an existing connection', () => {
    cy.visit('/cdap/ns/default/connections');
    cy.get(
      `[data-cy="connection-action-popover-toggle-${ConnectionType.GCS}-${DEFAULT_GCS_CONNECTION_NAME}"`
    ).click();
    cy.get(`[data-cy="wrangler-${ConnectionType.GCS}-connection-delete"]`).click();
    cy.contains('Are you sure you want to delete connection');
    cy.get(`[data-cy="wrangler-${ConnectionType.GCS}-delete-confirmation-btn"]`).click();
    cy.get(
      `[data-cy="wrangler-${ConnectionType.GCS}-connection-${DEFAULT_GCS_CONNECTION_NAME}"]`
    ).should('not.exist');
  });
});
