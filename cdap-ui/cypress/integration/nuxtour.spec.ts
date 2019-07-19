/*
 * Copyright © 2019 Cask Data, Inc.
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

import * as Helpers from '../helpers';

let headers = {};
const controlCenterText = 'Control Center';

// Assumes 5 step tour: Control Center, Pipeline Studio, Wrangler, Metadata, Hub

describe('NUX tour tests', () => {
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
      // cy.request({
      //   method: 'GET',
      //   url: `http://${Cypress.env('host')}:11015/v3/configuration/user`,
      // }).then((res) => {
      //   expect(res.body).to.not.have.property('showWelcome');
      // });
    });
  });
  after(() => {
    cy.request({
      method: 'PUT',
      url: `http://${Cypress.env('host')}:11015/v3/configuration/user`,
      body: { property: {} },
      headers,
    });
  });
  beforeEach(() => {
    cy.visit('/cdap', {
      onBeforeLoad: (win) => {
        win.sessionStorage.clear();
      },
    });
  });

  it('Should show 5 step tour when user first visits homepage', () => {
    cy.get('[data-cy="welcome-nux-tour"]').should('exist');
    cy.get('[data-cy="start-tour-btn"]').click();

    cy.get('.shepherd-title').then((modalText) => {
      expect(modalText).to.contain(controlCenterText);
    });

    cy.get('[data-id="control-center"]').within(() => {
      cy.get('.next-btn').click();
    });

    cy.get('.shepherd-title').then((modal) => {
      const pipelinesText = 'Pipeline Studio';
      expect(modal).to.contain(pipelinesText);
    });

    cy.get('[data-id="pipelines"]').within(() => {
      cy.get('.next-btn').click();
    });

    cy.get('.shepherd-title').then((modal) => {
      const dataPrepText = 'Wrangler';
      expect(modal).to.contain(dataPrepText);
    });

    cy.get('[data-id="preparation"]').within(() => {
      cy.get('.next-btn').click();
    });

    cy.get('.shepherd-title').then((modal) => {
      const metadataText = 'Metadata';
      expect(modal).to.contain(metadataText);
    });

    cy.get('[data-id="metadata"]').within(() => {
      cy.get('.next-btn').click();
    });

    cy.get('.shepherd-title').then((modal) => {
      const hubText = 'Hub';
      expect(modal).to.contain(hubText);
    });

    cy.get('[data-id="hub"]').within(() => {
      cy.get('.complete-btn')
        .click()
        .then(() => {
          cy.get('.guided-tour-tooltip').should('not.exist');
        });
    });
  });

  it('Should not show Welcome modal when user navigates to Angular page (Pipelines) and back', () => {
    cy.visit('/cdap/pipelines');
    cy.visit('/cdap');
    cy.get('[data-cy="welcome-nux-tour"]').should('not.exist');
  });

  it('Should allow user to use Previous and Cancel to navigate/exit tour', () => {
    cy.get('[data-cy="welcome-nux-tour"]').should('exist');
    cy.get('[data-cy="start-tour-btn"]').click();

    cy.get('[data-id="control-center"]').within(() => {
      cy.get('.next-btn').click();
    });

    cy.get('[data-id="pipelines"]').within(() => {
      cy.get('.prev-btn').click();
    });

    cy.get('.shepherd-title').then((modalText) => {
      expect(modalText).to.contain(controlCenterText);
    });

    cy.get('[data-id="control-center"]').within(() => {
      cy.get('.shepherd-cancel-link').click();
    });

    cy.get('.guided-tour-tooltip').should('not.exist');
  });

  it('Should allow user to close tour using No Thanks button', () => {
    cy.get('[data-cy="welcome-nux-tour"]').should('exist');

    cy.get('[data-cy="no-tour-btn"]')
      .click()
      .then(() => {
        cy.get('[data-cy="welcome-nux-tour"]').should('not.exist');
      });
  });

  it('Should allow user to click checkbox to opt out of future tours', () => {
    cy.get('[data-cy="welcome-nux-tour"]').should('exist');

    cy.get('[data-cy="show-again-checkbox"]')
      .click()
      .then(() => {
        cy.get('.icon-close').click();
        cy.get('[data-cy="welcome-nux-tour"]').should('not.exist');
      });
    cy.visit('/cdap/pipelines');
    cy.visit('/cdap');
    cy.get('[data-cy="welcome-nux-tour"]').should('not.exist');
  });
});
