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
  DEFAULT_SPANNER_CONNECTION_NAME,
  DEFAULT_SPANNER_DATABASE,
  DEFAULT_SPANNER_INSTANCE,
  DEFAULT_SPANNER_TABLE,
} from '../support/constants';

let headers;

describe('Wrangler SPANNER tests', () => {
  before(() => {
    Helpers.loginIfRequired()
      .then(() => {
        cy.getCookie('CDAP_Auth_Token').then((cookie) => {
          if (!cookie) {
            return Helpers.getSessionToken({});
          }
          headers = {
            Authorization: 'Bearer ' + cookie.value,
          };
          return Helpers.getSessionToken(headers);
        });
      })
      .then(sessionToken => headers = Object.assign({}, headers, { 'Session-Token': sessionToken }))
      .then(() => cy.start_wrangler(headers));
  });

  it('Should successfully test SPANNER connection', () => {
    cy.test_SPANNER_connection(DEFAULT_SPANNER_CONNECTION_NAME);
    cy.get('.card-action-feedback.SUCCESS');
    cy.contains('Test connection successful');
  });

  it('Should show appropriate message when test connection fails', () => {
    cy.test_SPANNER_connection('unknown_spanner_connection', 'unknown_project', 'unknown_path');
    cy.get('.card-action-feedback.DANGER');
    cy.contains('Test connection failed');
  });

  it('Should create SPANNER connection', () => {
    cy.create_SPANNER_connection(DEFAULT_SPANNER_CONNECTION_NAME);
    cy.get(`[data-cy="wrangler-SPANNER-connection-${DEFAULT_SPANNER_CONNECTION_NAME}"]`);
    cy.get(
      `[data-cy="wrangler-${ConnectionType.SPANNER}-connection-${DEFAULT_SPANNER_CONNECTION_NAME}"]`
    );
  });

  it('Should show proper error message when trying to create existing connection', () => {
    cy.create_SPANNER_connection(DEFAULT_SPANNER_CONNECTION_NAME);
    cy.get('.card-action-feedback.DANGER');
    cy.get('.card-action-feedback.DANGER .main-message .expand-icon').click();
    cy.get('.card-action-feedback.DANGER .stack-trace').should(
      'contain',
      `'${DEFAULT_SPANNER_CONNECTION_NAME}' already exists.`
    );
  });

  it('Should be able navigate inside SPANNER connection & create workspace', () => {
    cy.visit('/cdap/ns/default/connections');
    cy.get(
      `[data-cy="wrangler-${ConnectionType.SPANNER}-connection-${DEFAULT_SPANNER_CONNECTION_NAME}"]`
    ).click();
    cy.get('.spanner-browser .list-view-container .table-body')
      .contains(DEFAULT_SPANNER_INSTANCE)
      .click();
    cy.get('.spanner-browser .list-view-container .table-body')
      .contains(DEFAULT_SPANNER_DATABASE)
      .click();
    cy.get('.spanner-browser .list-view-container .table-body')
      .contains(DEFAULT_SPANNER_TABLE)
      .click();
    cy.url().should('contain', '/ns/default/wrangler');
  });

  it('Should show appropriate error when navigating to incorrect SPANNER connection', () => {
    const connName = 'spanner_unknown_connection';
    cy.visit(`/cdap/ns/default/connections/spanner/${connName}`);
    cy.contains(`Connection '${connName}' does not exist`);
    cy.contains('No instances in connection');
  });

  it('Should delete an existing connection', () => {
    cy.visit('/cdap/ns/default/connections');
    cy.get(
      `[data-cy="connection-action-popover-toggle-${
      ConnectionType.SPANNER
      }-${DEFAULT_SPANNER_CONNECTION_NAME}"`
    ).click();
    cy.get(`[data-cy="wrangler-${ConnectionType.SPANNER}-connection-delete"]`).click();
    cy.contains('Are you sure you want to delete connection');
    cy.get(`[data-cy="wrangler-${ConnectionType.SPANNER}-delete-confirmation-btn"]`).click();
    cy.get(
      `[data-cy="wrangler-${ConnectionType.SPANNER}-connection-${DEFAULT_SPANNER_CONNECTION_NAME}"]`
    ).should('not.exist');
  });
});
