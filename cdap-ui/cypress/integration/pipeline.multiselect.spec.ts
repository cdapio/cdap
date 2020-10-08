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

  it('Should select node(s)', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.create_simple_pipeline().then(({ sourceNodeId, transformNodeId, sinkNodeId }) => {
      cy.select_from_to(sourceNodeId, transformNodeId);
      cy.get('.box.selected').should('have.length', 2);
      cy.get('#dag-container').click();
      cy.select_from_to(sourceNodeId, sinkNodeId);
      cy.get('.box.selected').should('have.length', 3);
      cy.get('#dag-container').click({ force: true });
      cy.get('.box.selected').should('have.length', 0);
    });
  });

  it('Should select connection(s)', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.create_simple_pipeline().then(({ sourceNodeId, transformNodeId, sinkNodeId }) => {
      cy.select_connection(sourceNodeId, transformNodeId).then((element) => {
        expect(element[0].children[1].getAttribute('stroke')).to.eq('#58b7f6');
      });
      cy.select_connection(transformNodeId, sinkNodeId).then((element) => {
        expect(element[0].children[1].getAttribute('stroke')).to.eq('#58b7f6');
      });
      cy.get('body').type('{del}');
      cy.get_pipeline_json().then((pipelineConfig) => {
        const connections = pipelineConfig.config.connections;
        expect(connections.length).eq(0);
      });
    });
  });

  it('Should not select if not in selection mode', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.create_simple_pipeline().then(({ sourceNodeId: from, transformNodeId: to }) => {
      let fromNodeElement;
      let toNodeElement;
      cy.get_node(from).then((sElement) => {
        fromNodeElement = sElement;
        cy.get_node(to).then((tElement) => {
          toNodeElement = tElement;
          const { x: fromX, y: fromY } = fromNodeElement[0].getBoundingClientRect();
          const {
            x: toX,
            y: toY,
            width: toWidth,
            height: toHeight,
          } = toNodeElement[0].getBoundingClientRect();
          cy.get('[data-cy="pipeline-move-mdoe-action-btn"]').click();
          cy.get('#dag-container').trigger('mousedown', {
            which: 1,
            clientX: fromX - 10,
            clientY: fromY - 10,
          });
          cy.get('#dag-container')
            .trigger('mousemove', {
              which: 1,
              clientX: toX + toWidth + 10,
              clientY: toY + toHeight + 10,
            })
            .trigger('mouseup', { force: true });
          cy.get('.box.selected').should('have.length', 0);
          cy.get('[data-cy="pipeline-move-mdoe-action-btn"]').click();
        });
      });
    });
  });

  it('Should select everything including edges', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.create_complex_pipeline().then(
      ({ sourceNodeId1, transformNodeId2, joinerNodeId, sinkNodeId2 }) => {
        cy.get('[data-cy="feature-heading"]').click();
        cy.select_from_to(sourceNodeId1, transformNodeId2);
        cy.get('.box.selected').should('have.length', 4);
        cy.get('svg.selected-connector').should('have.length', 2);
        cy.get('#dag-container').click();
        cy.select_from_to(joinerNodeId, sinkNodeId2);
        cy.get('svg.selected-connector').should('have.length', 3);
      }
    );
  });

  it('Should correctly copy/paste/delete only nodes/connections in the selection', () => {
    cy.visit('/pipelines/ns/default/studio');
    if (Cypress.env('host') !== 'localhost') {
      return;
    }
    cy.create_simple_pipeline().then(({ sourceNodeId, transformNodeId, sinkNodeId }) => {
      cy.select_from_to(sourceNodeId, transformNodeId);
      cy.get(Helpers.getNodeSelectorFromNodeIndentifier(sourceNodeId)).rightclick();
      cy.get('[data-cy="menu-item-plugin copy"]:visible').click();
      cy.get('#dag-container').rightclick({ force: true });
      cy.get('[data-cy="menu-item-pipeline-node-paste"]').click();
      cy.get_pipeline_json().then((pipelineConfig) => {
        const connections = pipelineConfig.config.connections;
        const stages = pipelineConfig.config.stages;
        expect(connections.length).eq(3);
        expect(stages.length).eq(5);
      });
      const undoSelector = '[data-cy="pipeline-undo-action-btn"]';
      cy.get(undoSelector).click();
      cy.get_pipeline_json().then((pipelineConfig) => {
        const connections = pipelineConfig.config.connections;
        const stages = pipelineConfig.config.stages;
        expect(connections.length).eq(2);
        expect(stages.length).eq(3);
      });
    });
  });
});
