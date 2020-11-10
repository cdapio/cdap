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

import { loginIfRequired,  } from '../helpers';
import { INodeInfo, INodeIdentifier } from '../typings';

let headers = {};

describe('Code editor widget', () => { 
  const js: INodeInfo = { nodeName: 'JavaScript', nodeType: 'transform' };
  const jsId: INodeIdentifier = { ...js, nodeId: '0' };
  const defaultJsEditorVal = `
  /**
   * @summary Transforms the provided input record into zero or more output records or errors.
  
   * Input records are available in JavaScript code as JSON objects. 
  
   * @param input an object that contains the input record as a JSON.   e.g. to access a field called 'total' from the input record, use input.total.
   * @param emitter an object that can be used to emit zero or more records (using the emitter.emit() method) or errors (using the emitter.emitError() method) 
   * @param context an object that provides access to:
   *            1. CDAP Metrics - context.getMetrics().count('output', 1);
   *            2. CDAP Logs - context.getLogger().debug('Received a record');
   *            3. Lookups - context.getLookup('blacklist').lookup(input.id); or
   *            4. Runtime Arguments - context.getArguments().get('priceThreshold') 
   */ 
  function transform(input, emitter, context) {
    emitter.emit(input);
  }
  `;

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

  it('Should render default value the first time', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.open_transform_panel();
    cy.add_node_to_canvas(js);
    cy.open_node_property(jsId);
    cy.window().then((win) => {
      cy.get('.ace-editor-ref').then((aceElement) => {
        const jsEditorValue = win.ace.edit(aceElement[0]).getValue();
        expect(jsEditorValue.replace(/[\n ]/g, '')).to.equal(defaultJsEditorVal.replace(/[\n ]/g, ''));
      });
    });
  });

  it('Should not jump the cursor position on selecting text and replacing them', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.open_transform_panel();
    cy.add_node_to_canvas(js);
    cy.open_node_property(jsId);
    cy.window().then((win) => {
      cy.get('.ace-editor-ref').then((aceElement) => {
        const aceEditor = win.ace.edit(aceElement[0]);
        aceEditor.gotoLine(15);
        const rangeToReplace = {
          start: {row: 14, column: 0},
          end: {row: 14, column: 30}
        };
        aceEditor.session.doc.replace(rangeToReplace, 'console.log("newcode");');
        const newValue = aceEditor.getValue();
        aceEditor.resize();
        cy.wrap(newValue).then(() => {
          expect(newValue).contains('console.log("newcode");');
          expect(aceEditor.getCursorPosition()).to.deep.equal({
            row: 14,
            column: 23
          });
        });
      });
    });
    
  });
});
