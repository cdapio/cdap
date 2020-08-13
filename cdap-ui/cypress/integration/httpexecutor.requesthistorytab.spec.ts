/*
 * Copyright © 2020 Cask Data, Inc.
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

const MOCK_LOCAL_STORAGE = JSON.stringify([
  {
    id: '1',
    method: 'GET',
    path: 'hello.com',
    body: 'hello',
    headers: { pairs: [{ key: '', value: '', uniqueId: '6fd93a6e-5b2f-47aa-8143-157629bdf457' }] },
    response: 'Problem accessing: /v3/hello.com. Reason: Not Found',
    statusCode: 404,
    timestamp: '7/5/2020, 6:54:19 PM',
  },
  {
    id: '2',
    method: 'DELETE',
    path: 'hello2.com',
    body: 'hello2',
    headers: { pairs: [{ key: '', value: '', uniqueId: '6fd93a6e-5b2f-47aa-8143-157629bdf457' }] },
    response: 'Problem accessing: /v3/hello2.com. Reason: Not Found',
    statusCode: 404,
    timestamp: '7/6/2020, 6:54:19 PM',
  },
  {
    id: '3',
    method: 'POST',
    path: 'hello3.com',
    body: 'hello3',
    headers: { pairs: [{ key: '', value: '', uniqueId: '6fd93a6e-5b2f-47aa-8143-157629bdf457' }] },
    response: 'Problem accessing: /v3/hello3.com. Reason: Not Found',
    statusCode: 409,
    timestamp: '7/7/2020, 6:54:19 PM',
  },
]);

describe('RequestHistoryTab in httpExecutor', () => {
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

  describe('Accessing and managing secure keys', () => {
    before(() => {
      // Set local storage before visiting the page
      localStorage.setItem('RequestHistory', MOCK_LOCAL_STORAGE);
      cy.visit('/httpexecutor');
    });

    it('should show populate requestHistoryTab with existing items in localStorage', () => {
      JSON.parse(MOCK_LOCAL_STORAGE).forEach((req) => {
        const ID = req.id;

        cy.get(dataCy(`request-row-${ID}`)).should('exist');

        cy.get(`${dataCy(`request-row-${ID}`)} ${dataCy('request-path')}`)
          .invoke('text')
          .should((text) => {
            expect(text).to.eq(req.path);
          });
        cy.get(`${dataCy(`request-row-${ID}`)} ${dataCy('request-method')}`)
          .invoke('text')
          .should((text) => {
            expect(text).to.eq(req.method);
          });

        // Check if main page has been populated correctly
        cy.get(dataCy(`request-row-${ID}`)).click();
        cy.get(dataCy('request-method-selector'))
          .invoke('val')
          .should((val) => {
            expect(val).to.eq(req.method);
          });
        cy.get(dataCy('request-path-input'))
          .invoke('val')
          .should((val) => {
            expect(val).to.eq(req.path);
          });
        cy.get(dataCy('response-status-code'))
          .invoke('text')
          .should((text) => {
            expect(text).to.eq(req.statusCode.toString());
          });
        cy.get(dataCy('response'))
          .invoke('text')
          .should((text) => {
            expect(text).to.eq(req.response);
          });
        if (req.method === 'POST') {
          cy.get(dataCy('body-btn')).click();
          cy.get(dataCy('request-body'))
            .invoke('val')
            .should((val) => {
              expect(val).to.eq(req.body);
            });
        }
      });
    });

    it('should search for requests from RequestHistoryTab', () => {
      // All the requests include searchText1 in their path,
      // validate every request appears in requestHistoryTab
      const searchText1 = 'hello';
      cy.get(dataCy('request-search-input'))
        .click()
        .focused()
        .clear()
        .type(searchText1);
      cy.get('[data-cy*="request-row"]').should(
        'have.length',
        JSON.parse(MOCK_LOCAL_STORAGE).length
      );

      // Only one request include searchText2 in their path
      const searchText2 = 'hello.com';
      cy.get(dataCy('request-search-input'))
        .click()
        .focused()
        .clear()
        .type(searchText2);
      cy.get('[data-cy*="request-row"]').should('have.length', 1);

      // None of the requests include searchText3 in their path
      const searchText3 = 'hellooo';
      cy.get(dataCy('request-search-input'))
        .click()
        .focused()
        .clear()
        .type(searchText3);
      cy.get('[data-cy*="request-row"]').should('have.length', 0);

      // Clear out all the search text
      cy.get(dataCy('request-search-input'))
        .click()
        .focused()
        .clear();
    });

    it('should add a request to requestHistoryTab', () => {
      // Should add a new request when the save mode is off
      cy.get(dataCy('save-mode-btn')).click(); // turn off the save mode
      const newRequest1 = {
        method: 'DELETE',
        path: 'https://new-request-1.com',
      };
      // Attempt to add a new request
      cy.get(dataCy('request-method-selector')).select(newRequest1.method);
      cy.get(dataCy('request-path-input'))
        .clear()
        .click()
        .type(newRequest1.path);
      cy.get(dataCy('send-btn')).click();
      // Validate a new request has NOT been added to requestHistoryTab
      cy.get('[data-cy*="request-row"]').should(
        'have.length',
        JSON.parse(MOCK_LOCAL_STORAGE).length
      );

      // Should add a new request when the save mode is on
      cy.get(dataCy('save-mode-btn')).click(); // turn on the save mode
      const newRequest2 = {
        method: 'POST',
        path: 'https://new-request-2.com',
        body: 'hello4',
      };
      // Attempt to add a new request
      cy.get(dataCy('request-method-selector')).select(newRequest2.method);
      cy.get(dataCy('request-path-input'))
        .clear()
        .click()
        .type(newRequest2.path);
      cy.get(dataCy('request-body'))
        .clear()
        .click()
        .type(newRequest2.body);
      cy.get(dataCy('send-btn')).click();
      // Validate a new request has been added to requestHistoryTab
      cy.get('[data-cy*="request-row"]').should(
        'have.length',
        JSON.parse(MOCK_LOCAL_STORAGE).length + 1
      );
    });

    it('should delete a request from RequestHistoryTab', () => {
      // Delete the latest request from RequestHistoryTab
      cy.get('[data-cy*="request-row"]').within(() => {
        // Since delete-icon is a hidden element, we need click({force: true})
        cy.get(dataCy('delete-icon'))
          .first() // the first element has the latest request
          .invoke('show')
          .click({ force: true });
      });

      // Check whether a delete dialog has been opened
      cy.get(dataCy('confirm-dialog')).should('exist');

      // Confirm delete
      cy.get(dataCy('Delete')).click();

      // Validate the request has been deleted from requestHistoryTab
      cy.get('[data-cy*="request-row"]').should(
        'have.length',
        JSON.parse(MOCK_LOCAL_STORAGE).length // back to the original length since the previous test added another request
      );
    });

    it('should clear all requests in RequestHistoryTab', () => {
      cy.get(dataCy('clear-btn')).click({ force: true }); // clear all the requests

      // Check whether a clear dialog has been opened
      cy.get(dataCy('confirm-dialog')).should('exist');

      // Confirm clear
      cy.get(dataCy('Clear All')).click();

      // Validate all the requests has been cleared from requestHistoryTab
      cy.get('[data-cy*="request-row"]').should('have.length', 0);
    });
  });
});
