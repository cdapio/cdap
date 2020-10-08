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

import { loginIfRequired, getArtifactsPoll } from '../helpers';
import { INodeInfo, INodeIdentifier } from '../typings';
import { dataCy } from '../helpers';

let headers = {};

describe('CSV Widgets', () => {
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

  const property = 'drop';
  const projection: INodeInfo = { nodeName: 'Projection', nodeType: 'transform' };
  const projectionId: INodeIdentifier = { ...projection, nodeId: '0' };

  const propertySelector = dataCy(property);
  const row1Selector = `${propertySelector} ${dataCy(0)}`;
  const row2Selector = `${propertySelector} ${dataCy(1)}`;

  beforeEach(() => {
    getArtifactsPoll(headers);
  });

  it('Should render csv row', () => {
    cy.visit('/pipelines/ns/default/studio');

    // add plugin to canvas
    cy.open_transform_panel();
    cy.add_node_to_canvas(projection);

    cy.open_node_property(projectionId);

    cy.get(propertySelector).should('exist');
    cy.get(row1Selector).should('exist');
    cy.get(row2Selector).should('not.exist');
  });

  it('Should add a new row', () => {
    cy.get(`${row1Selector} ${dataCy('add-row')}`).click();
    cy.get(`${row2Selector}`).should('exist');
  });

  it('Should input property', () => {
    cy.get(`${row1Selector} ${dataCy('key')}`).type('value1');
    cy.get(`${row2Selector} ${dataCy('key')}`).type('value2');

    cy.close_node_property();

    cy.get_pipeline_stage_json(projectionId.nodeName).then((stage) => {
      const stageProperties = stage.plugin.properties;
      expect(stageProperties[property]).equals('value1,value2');
    });
  });

  it('Should re-render existing property', () => {
    cy.open_node_property(projectionId);

    cy.get(row1Selector).should('exist');
    cy.get(row2Selector).should('exist');
    cy.get(`${row1Selector} ${dataCy('key')} input`)
      .invoke('val')
      .then((val) => {
        expect(val).equals('value1');
      });
  });

  it('Should delete property', () => {
    cy.get(`${row2Selector} ${dataCy('remove-row')}`).click();
    cy.get(`${row2Selector}`).should('not.exist');

    cy.close_node_property();

    cy.get_pipeline_stage_json(projectionId.nodeName).then((stage) => {
      const stageProperties = stage.plugin.properties;
      expect(stageProperties[property]).equals('value1');
    });
  });
});
