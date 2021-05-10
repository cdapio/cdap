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

import { loginIfRequired, getArtifactsPoll, dataCy, getGenericEndpoint } from '../helpers';
import { INodeInfo, INodeIdentifier } from '../typings';
let headers = {};

describe('Hierarchy Widgets', () => {
  const property = 'fieldMapping';
  const createRecord: INodeInfo = { nodeName: 'CreateRecord', nodeType: 'transform' };
  const createRecordId: INodeIdentifier = { ...createRecord, nodeId: '1' };
  const fileSource: INodeInfo = { nodeName: 'File', nodeType: 'batchsource' };
  const fileSourceId: INodeIdentifier = { ...fileSource, nodeId: '0' };
  const propertySelector = dataCy(property);
  const row1Selector = `${propertySelector}`;

  const addField = (row, name, type = null) => {
    cy.get(`[data-cy="schema-row-${row}"] input[placeholder="Field name"]`)
      .type(name)
      .type('{enter}');
    return type ? cy.get(`[data-cy="schema-row-${row}"] select`).select(type) : cy.wrap(true);
  };
  const removeField = (row) => {
    return cy.get(`[data-cy="schema-row-${row}"] [data-cy="schema-field-remove-button"]`).click();
  };

  before(() => {
    loginIfRequired().then(() => {
      cy.getCookie('CDAP_Auth_Token').then((cookie) => {
        if (!cookie) {
          return;
        }
        headers = {
          Authorization: `Bearer ${cookie.value}`,
        };
      });
    });
  });

  it('Should render File and Create Record', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.add_node_to_canvas(fileSource)
    cy.open_transform_panel();
    cy.add_node_to_canvas(createRecord);
    cy.pipeline_clean_up_graph_control();
    cy.fit_pipeline_to_screen();
    cy.connect_two_nodes(fileSourceId, createRecordId, getGenericEndpoint);
    cy.open_node_property(fileSourceId);
  })

  it('Should add schema with simple files', () => {
    removeField(0)
    removeField(0)
    addField(0, 'column1');
    addField(1, 'column2');
    addField(2, 'column3');
    addField(3, 'column4');
    addField(4, 'column5');
    addField(5, 'column6');
    addField(6, 'column7');
    addField(7, 'column8');
    addField(8, 'column9');
    addField(9, 'column10');
    cy.close_node_property();
  });

  it('Should render Create Record', () => {
    cy.open_node_property(createRecordId);
    cy.get(propertySelector).should('exist');
  });

  it('Should add a new row', () => {
    cy.get(`${row1Selector} ${dataCy('add')}`).click();
    cy.get(`${row1Selector}`).should('exist');
  });

  it('Should input some properties', () => {
    cy.get(`${row1Selector} ${dataCy('input')}`).type('test1');
    cy.get(`${row1Selector} ${dataCy('add-popup')}`).click();
    cy.get(`${dataCy('add-child')}`).click();
    cy.get(`${row1Selector} ${dataCy('autocomplete-input')}`).click();
    cy.get(`${row1Selector} ${dataCy('autocomplete-input')}`).type('column2');
    cy.get(`${dataCy('option-column2')}`).click();
    cy.get(`${dataCy('option-column4')}`).click();
    cy.get(`${dataCy('option-column8')}`).click();
  });
  
  it('Should get the schema', () => {
    cy.get('[data-cy=plugin-undefined] > [data-cy=widget-wrapper-container] > .WidgetWrapperView-widgetContainer-607 > .abstract-widget-wrapper > div').click()
    cy.get(`${dataCy('get-schema-btn')}`).click();
  });
});
