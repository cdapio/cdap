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
  DEFAULT_BIGQUERY_CONNECTION_NAME,
  DEFAULT_BIGQUERY_DATASET,
  DEFAULT_BIGQUERY_TABLE,
} from '../support/constants';

let headers;

describe('Wrangler BigQuery tests', () => {
  before(() => {
    return Helpers.loginIfRequired()
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
        return cy.wrap(headers);
      })
      .then(sessionToken => {
        headers = Object.assign({}, headers, { 'Session-Token': sessionToken });
        return cy.wrap(headers);
      })
      .then(() => cy.start_wrangler(headers));
  });
  it('Should successfully test BigQuery connection', () => {
    cy.test_BIGQUERY_connection(DEFAULT_BIGQUERY_CONNECTION_NAME);
    cy.get('.card-action-feedback.SUCCESS');
    cy.contains('Test connection successful');
  });

  it('Should show appropriate message when test connection fails', () => {
    cy.test_BIGQUERY_connection(
      'invalid_connection',
      'invalid_projectid',
      'invalid_serviceaccount_path'
    );
    cy.get('.card-action-feedback.DANGER');
    cy.contains('Test connection failed');
  });

  it('Should create BigQuery connection', () => {
    cy.create_BIGQUERY_connection(DEFAULT_BIGQUERY_CONNECTION_NAME);
    cy.get(
      `[data-cy="wrangler-${
      ConnectionType.BIGQUERY
      }-connection-${DEFAULT_BIGQUERY_CONNECTION_NAME}"]`
    );
  });
  it('Should show proper error message when trying to create existing connection', () => {
    cy.create_BIGQUERY_connection(DEFAULT_BIGQUERY_CONNECTION_NAME);
    cy.get('.card-action-feedback.DANGER');
    cy.get('.card-action-feedback.DANGER .main-message .expand-icon').click();
    cy.get('.card-action-feedback.DANGER .stack-trace').should(
      'contain',
      `'${DEFAULT_BIGQUERY_CONNECTION_NAME}' already exists.`
    );
  });

  it('Should be able to navigate inside BigQuery and create a workspace', () => {
    cy.visit('/cdap/ns/default/connections');
    cy.get(
      `[data-cy="wrangler-${
      ConnectionType.BIGQUERY
      }-connection-${DEFAULT_BIGQUERY_CONNECTION_NAME}"]`
    ).click();
    cy.contains(DEFAULT_BIGQUERY_DATASET).click();
    cy.contains(DEFAULT_BIGQUERY_TABLE).click();
    cy.url().should('contain', '/ns/default/wrangler');
  });

  it('Should show appropriate error when navigating to incorrect BigQuery connection', () => {
    const connName = 'bigquery_unknown_connection';
    cy.visit(`/cdap/ns/default/connections/bigquery/${connName}`);
    cy.contains(`Connection '${connName}' does not exist`);
    cy.contains('No datasets in connection');
  });

  it('Should delete an existing connection', () => {
    cy.visit('/cdap/ns/default/connections');
    cy.get(
      `[data-cy="connection-action-popover-toggle-${
      ConnectionType.BIGQUERY
      }-${DEFAULT_BIGQUERY_CONNECTION_NAME}"`
    ).click();
    cy.get(`[data-cy="wrangler-${ConnectionType.BIGQUERY}-connection-delete"]`).click();
    cy.contains('Are you sure you want to delete connection');
    cy.get(`[data-cy="wrangler-${ConnectionType.BIGQUERY}-delete-confirmation-btn"]`).click();
    cy.get(
      `[data-cy="wrangler-${
      ConnectionType.BIGQUERY
      }-connection-${DEFAULT_BIGQUERY_CONNECTION_NAME}"]`
    ).should('not.exist');
  });
});
