/*
 * Copyright © 2020 Cask Data, Inc.
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
describe('Pipeline multi-select nodes + context menu for plugins & canvas', () => {
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
    cy.visit('/cdap', {
      onBeforeLoad: (win) => {
        win.sessionStorage.clear();
      },
    });
  });
  beforeEach(() => {
    Helpers.setNewSchemaEditor();
  });
  afterEach(() => {
    cy.clearLocalStorage();
  });

  it('Should show plugin level context menu', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.create_simple_pipeline().then(({ sourceNodeId }) => {
      cy.get(Helpers.getNodeSelectorFromNodeIndentifier(sourceNodeId)).rightclick();
      cy.get('li[role="menuitem"]').contains('Copy Plugin');
      cy.get('li[role="menuitem"]').contains('Delete Plugin');
      cy.get('body').type('{esc}', { release: true });
      cy.get('#dag-container').click({ force: true });
    });
  });
  it('Should work by clicking on plugin context menu options', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.create_simple_pipeline().then(({ sourceNodeId, sinkNodeId }) => {
      cy.select_from_to(sourceNodeId, sinkNodeId);
      cy.get(Helpers.getNodeSelectorFromNodeIndentifier(sourceNodeId)).rightclick();
      cy.get('li[role="menuitem"]').contains('Copy Plugins');
      cy.get('li[role="menuitem"]')
        .contains('Delete Plugins')
        .click();
      cy.get('[data-cy="pipeline-export-btn"]').should('have.attr', 'disabled', 'disabled');
      const undoSelector = '[data-cy="pipeline-undo-action-btn"]';
      cy.get(undoSelector).click();
      cy.get(undoSelector).click();
      cy.get(undoSelector).click();
      cy.get_pipeline_json().then((pipelineConfig) => {
        const connections = pipelineConfig.config.connections;
        const stages = pipelineConfig.config.stages;
        expect(connections.length).eq(2);
        expect(stages.length).eq(3);
      });
    });
  });

  it('Should show pipeline level icon', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.create_simple_pipeline().then(() => {
      cy.get('#dag-container').rightclick({ force: true });
      cy.get('li[role="menuitem"]').contains('Wrangle');
      cy.get('li[role="menuitem"]').contains('Zoom In');
      cy.get('li[role="menuitem"]').contains('Zoom Out');
      cy.get('li[role="menuitem"]').contains('Fit to Screen');
      cy.get('li[role="menuitem"]').contains('Align');
      cy.get('li[role="menuitem"]').contains('Paste');
    });
  });

  it('Should work on clicking zoom-in/out actions', () => {
    cy.get('#dag-container').then((el) => {
      // matrix(1,0,0,1,0,0) is the default value for transform
      // with scalex & y(zoom) to be 1 and dx & dy to be 0
      expect(el).to.have.css('transform', 'matrix(1, 0, 0, 1, 0, 0)');
    });
    cy.get('#dag-container').rightclick({ force: true });
    cy.get('[data-cy="menu-item-zoom-in"]').click();
    cy.get('#dag-container').then((el) => {
      expect(el).to.have.css('transform', 'matrix(1.1, 0, 0, 1.1, 0, 0)');
    });
    cy.get('#dag-container').rightclick({ force: true });
    cy.get('[data-cy="menu-item-zoom-in"]').click();
    cy.get('#dag-container').then((el) => {
      expect(el).to.have.css('transform', 'matrix(1.2, 0, 0, 1.2, 0, 0)');
    });
    cy.get('#dag-container').rightclick({ force: true });
    cy.get('[data-cy="menu-item-zoom-out"]').click();
    cy.get('#dag-container').rightclick({ force: true });
    cy.get('[data-cy="menu-item-zoom-out"]').click();
    cy.get('#dag-container').then((el) => {
      expect(el).to.have.css('transform', 'matrix(1, 0, 0, 1, 0, 0)');
    });
  });

  it('Should work on clicking fit-to-screen', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.create_complex_pipeline().then(
      ({ sourceNodeId1, sourceNodeId2, sinkNodeId1, sinkNodeId2 }) => {
        cy.get_node(sourceNodeId1).should('be.visible');
        cy.get_node(sourceNodeId2).should('be.visible');
        cy.get_node(sinkNodeId1).should('be.visible');
        cy.get_node(sinkNodeId2).should('be.visible');
        cy.get('[data-cy="pipeline-zoom-in-control"]').click();
        cy.get('[data-cy="pipeline-zoom-in-control"]').click();
        cy.get('[data-cy="pipeline-zoom-in-control"]').click();
        cy.get('[data-cy="pipeline-zoom-in-control"]').click();
        cy.get_node(sourceNodeId1).should('not.be.visible');
        cy.get_node(sourceNodeId2).should('not.be.visible');
        cy.get_node(sinkNodeId1).should('not.be.visible');
        cy.get_node(sinkNodeId2).should('not.be.visible');
        cy.get('#dag-container').rightclick({ force: true });
        cy.get('[data-cy="menu-item-fit-to-screen"]').click();
        cy.get_node(sourceNodeId1).should('be.visible');
        cy.get_node(sourceNodeId2).should('be.visible');
        cy.get_node(sinkNodeId1).should('be.visible');
        cy.get_node(sinkNodeId2).should('be.visible');
      }
    );
  });

  it('Should be enabled/disabled if clipboard has valid pipeline object', () => {
    if (Cypress.env('host') !== 'localhost') {
      return;
    }
    cy.visit('/pipelines/ns/default/studio');
    cy.create_simple_pipeline().then(({ sourceNodeId, sinkNodeId }) => {
      cy.window().then((window) => window.CaskCommon.Clipboard.copyToClipBoard(''));
      cy.get('#dag-container').rightclick({ force: true });
      cy.get('[data-cy="menu-item-pipeline-node-paste"]').then((el) => {
        expect(el[0].getAttribute('aria-disabled')).to.eq('true');
      });
      cy.get('body').type('{esc}', { release: true });
      cy.select_from_to(sourceNodeId, sinkNodeId);
      cy.get(Helpers.getNodeSelectorFromNodeIndentifier(sourceNodeId)).rightclick();
      cy.get('[data-cy="menu-item-plugin copy"]:visible').click({ force: true });
      cy.get('#dag-container').rightclick({ force: true });
      cy.get('[data-cy="menu-item-pipeline-node-paste"]').then((el) => {
        expect(el[0].getAttribute('aria-disabled')).to.eq('false');
      });
    });
  });

  it('Should paste valid pipeline/plugins on clicking paste', () => {
    if (Cypress.env('host') !== 'localhost') {
      return;
    }
    cy.visit('/pipelines/ns/default/studio');
    cy.create_simple_pipeline().then(({ sourceNodeId, sinkNodeId }) => {
      cy.select_from_to(sourceNodeId, sinkNodeId);
      cy.get(Helpers.getNodeSelectorFromNodeIndentifier(sourceNodeId)).rightclick();
      cy.get('[data-cy="menu-item-plugin copy"]:visible').click();
      cy.get('#dag-container').rightclick({ force: true });
      cy.get('[data-cy="menu-item-pipeline-node-paste"]').click();
      cy.get_pipeline_json().then((pipelineConfig) => {
        const { stages, connections } = pipelineConfig.config;
        expect(stages.length).to.eq(6);
        expect(connections.length).to.eq(4);
      });
    });
  });

  it('Existing hamburger menu should work', () => {
    cy.visit('/pipelines/ns/default/studio');
    if (Cypress.env('host') !== 'localhost') {
      return;
    }
    cy.create_simple_pipeline().then(({ sourceNodeId }) => {
      const { nodeName, nodeType, nodeId } = sourceNodeId;
      const hamburgerSelector = `hamburgermenu-${nodeName}-${nodeType}-${nodeId}`;
      cy.get(`[data-cy="${hamburgerSelector}-toggle"]`).click();
      cy.get(`[data-cy="${hamburgerSelector}-copy"]`).click();
      cy.get('#dag-container').rightclick({ force: true });
      cy.get(`[data-cy="menu-item-pipeline-node-paste"]`).click();
      cy.get_pipeline_json().then((pipelineConfig) => {
        const { stages } = pipelineConfig.config;
        expect(stages.length).to.eq(4);
      });
      const newAddedSourceName = `BigQueryTable-batchsource-3`;
      cy.get(`[data-cy="hamburgermenu-${newAddedSourceName}-toggle"]`).click();
      cy.get(`[data-cy="hamburgermenu-${newAddedSourceName}-delete"]`).click();
      cy.get_pipeline_json().then((pipelineConfig) => {
        const { stages } = pipelineConfig.config;
        expect(stages.length).to.eq(3);
      });
    });
  });
});
