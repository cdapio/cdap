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

import { loginIfRequired, getArtifactsPoll } from '../helpers';
let headers = {};

describe('Pipeline Canvas actions', () => {
  // Uses API call to login instead of logging in manually through UI
  before(() => {
    loginIfRequired().then(() => {
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
    getArtifactsPoll(headers);
  });

  it('Should correctly undo/redo actions done by the user', () => {
    cy.visit('/pipelines/ns/default/studio');
    const undoSelector = '[data-cy="pipeline-undo-action-btn"]';
    const redoSelector = '[data-cy="pipeline-redo-action-btn"]';
    // During the initial load of studio these buttons should be disabled
    cy.get(redoSelector).should('have.attr', 'disabled', 'disabled');
    cy.get(undoSelector).should('have.attr', 'disabled', 'disabled');

    cy.create_simple_pipeline();
    cy.get(undoSelector).should('not.have.attr', 'disabled');
    cy.get(redoSelector).should('have.attr', 'disabled', 'disabled');

    // Undo the connections
    cy.get(undoSelector).click();
    cy.get(undoSelector).click();

    cy.get_pipeline_json().then((pipelineConfig) => {
      const connections = pipelineConfig.config.connections;
      expect(connections.length).eq(0);
    });

    // Redo one connection (connection between source & transform)
    cy.get(redoSelector).click();
    cy.get_pipeline_json().then((pipelineConfig) => {
      const connections = pipelineConfig.config.connections;
      expect(connections.length).eq(1);
    });

    // Undo everything.
    cy.get(undoSelector).click();
    cy.get(undoSelector).click();
    cy.get(undoSelector).click();
    cy.get(undoSelector).click();
    // By now it should be an empty canvas and the export button should be disabled.
    cy.get('[data-cy="pipeline-export-btn"]').should('have.attr', 'disabled', 'disabled');

    // Redo everything.
    cy.get(redoSelector).click();
    cy.get(redoSelector).click();
    cy.get(redoSelector).click();
    cy.get(redoSelector).click();
    cy.get(redoSelector).click();

    cy.get('[data-cy="pipeline-export-btn"]').should('not.have.attr', 'disabled');
    cy.get_pipeline_json().then((pipelineConfig) => {
      const connections = pipelineConfig.config.connections;
      const stages = pipelineConfig.config.stages;
      expect(connections.length).eq(2);
      expect(stages.length).eq(3);
    });
  });

  it('Should fit pipeline to screen and let user select multiple nodes', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.create_complex_pipeline();
    const sink1 = '[data-cy="plugin-node-BigQueryMultiTable-batchsink-7"]';
    const sink2 = '[data-cy="plugin-node-BigQueryMultiTable-batchsink-6"]';
    const zoombtn = '[data-cy="pipeline-zoom-in-control"]';
    const fitToScreen = '[data-cy="pipeline-fit-to-screen-control"]';
    const transform1 = '[data-cy="plugin-node-JavaScript-transform-2"]';
    const transform2 = '[data-cy="plugin-node-JavaScript-transform-3"]';
    const menuItemDelete = '[data-cy="menu-item-plugin delete"]:visible';
    const undoSelector = '[data-cy="pipeline-undo-action-btn"]';
    cy.get(sink1).should('be.visible');
    cy.get(sink2).should('be.visible');
    cy.get(zoombtn).click();
    cy.get(zoombtn).click();
    cy.get(zoombtn).click();
    cy.get(zoombtn).click();
    cy.get(zoombtn).click();
    cy.get(sink1).should('not.be.visible');
    cy.get(sink2).should('not.be.visible');

    cy.get(fitToScreen).click();
    cy.get(sink1).should('be.visible');
    cy.get(sink2).should('be.visible');

    cy.get(transform1)
      .trigger('mousedown', { pageX: 0, pageY: 0 })
      .trigger('mousemove', { pageX: -500, pageY: 500 })
      .trigger('mouseup', { force: true });
    cy.get(transform2)
      .trigger('mousedown', { pageX: 0, pageY: 0 })
      .trigger('mousemove', { pageX: 300, pageY: -500 })
      .trigger('mouseup', { force: true });
    cy.get(transform1).should('not.be.visible');
    cy.get(transform2).should('not.be.visible');

    cy.get(fitToScreen).click();

    cy.get(transform1).should('be.visible');
    cy.get(transform2).should('be.visible');

    // Use shift-click to select and delete two nodes
    cy.get(transform1).type('{shift}', { release: false });
    cy.get(transform2).click();
    cy.get(transform1).rightclick();
    cy.get(menuItemDelete).click();

    cy.get(transform1).should('not.exist');
    cy.get(transform2).should('not.exist');

    // Undo deleting nodes
    cy.get(undoSelector).click();
    cy.get(undoSelector).click();
  });

  it('Should align the pipeline correctly', () => {
    const cleanupbtn = '[data-cy="pipeline-clean-up-graph-control"]';
    const fitToScreen = '[data-cy="pipeline-fit-to-screen-control"]';
    cy.get(cleanupbtn).click();
    cy.get(fitToScreen).click();
    cy.get('[data-cy="feature-heading"]').click();
    cy.compareSnapshot('pipeline_with_condition');
  });
});
