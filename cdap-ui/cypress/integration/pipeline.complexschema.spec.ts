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

describe('Output Schema', () => {
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
  });

  const schemaFieldSuffix = '-schema-field';

  const projection: INodeInfo = { nodeName: 'Projection', nodeType: 'transform' };
  const projectionId: INodeIdentifier = { ...projection, nodeId: '0' };
  const addButton = dataCy('schema-row-add-button');
  const removeButton = dataCy('schema-row-remove-button');
  const schemaField = dataCy(schemaFieldSuffix);
  const schemaRow1 = dataCy('schema-row-0');
  const schemaRow2 = dataCy('schema-row-1');
  const schemaRow3 = dataCy('schema-row-2');

  beforeEach(() => {
    getArtifactsPoll(headers);
  });

  it('Should initialize with 1 empty row', () => {
    cy.visit('/pipelines/ns/default/studio');

    // add plugin to canvas
    cy.open_transform_panel();
    cy.add_node_to_canvas(projection);

    cy.open_node_property(projectionId);

    cy.get(schemaRow1).should('exist');
  });

  it('Should be able to add a new row', () => {
    cy.get(`${schemaRow1} ${schemaField}`).type('field1');
    cy.get(`${schemaRow1} ${addButton}`).click();

    cy.get(`${schemaRow2}`).should('exist');
    cy.get(`${schemaRow2} ${schemaField}`).should('have.focus');
    cy.get(`${schemaRow2} ${schemaField}`).type('another_field');
  });

  it('Should add new row directly underneath the current row', () => {
    cy.get(`${schemaRow1} ${addButton}`).click();
    cy.get(`${schemaRow2}`).should('exist');
    cy.get(`${schemaRow2} ${schemaField}`).should('have.focus');
    cy.get(`${schemaRow2} ${schemaField}`).type('middle');

    cy.get(`${schemaRow3} ${dataCy('another_field' + schemaFieldSuffix)}`)
      .invoke('val')
      .then((val) => {
        expect(val).equals('another_field');
      });
  });

  it('Should be able to remove row', () => {
    cy.get(`${schemaRow2} ${removeButton}`).click();

    cy.get(`${schemaRow3}`).should('not.exist');

    cy.get(`${schemaRow1} ${dataCy('field1' + schemaFieldSuffix)}`)
      .invoke('val')
      .then((val) => {
        expect(val).equals('field1');
      });

    cy.get(`${schemaRow2} ${dataCy('another_field' + schemaFieldSuffix)}`)
      .invoke('val')
      .then((val) => {
        expect(val).equals('another_field');
      });
  });

  it('Should retain value when node config is reopened', () => {
    cy.close_node_property();
    cy.open_node_property(projectionId);

    cy.get(`${schemaRow1} ${dataCy('field1' + schemaFieldSuffix)}`)
      .invoke('val')
      .then((val) => {
        expect(val).equals('field1');
      });

    cy.get(`${schemaRow2} ${dataCy('another_field' + schemaFieldSuffix)}`)
      .invoke('val')
      .then((val) => {
        expect(val).equals('another_field');
      });
  });
});
