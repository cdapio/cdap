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
import { INodeInfo, INodeIdentifier } from '../typings';

const nullSplitterPipeline = `null_splitter_pipeline_${Date.now()}`;
const unionSplitterPipeline = `union_splitter_${Date.now()}`;
const pipelines = [nullSplitterPipeline, unionSplitterPipeline];
const conditionPreviewMsg = 'Preview data is not supported for condition stages.';
const closeButton = '[data-testid="close-config-popover"]';
let headers = {};

// We don't have preview enabled in distributed mode.
// TODO(CDAP-16620) To enable preview in distributed mode. Once it is enabled we can remove.
let skipPreviewTests = false;
// @ts-ignore "cy.state" is not in the "cy" type
const getMochaContext = () => cy.state('runnable').ctx;
const skip = () => {
  const ctx = getMochaContext();
  return ctx.skip();
};

const addField = (row, name, type = null) => {
  cy.get(`[data-cy="Output Schema"] [data-cy="schema-row-${row}"] input[placeholder="Field name"]`)
    .clear()
    .type(name)
    .type('{enter}');
  return type ? cy.get(`[data-cy="schema-row-${row}"] select`).select(type) : cy.wrap(true);
};
const setFieldType = (row, type) => {
  cy.get(`[data-cy="Output Schema"] [data-cy="schema-row-${row}"] select`).select(type);
};

describe('Pipelines with plugins having more than one endpoints', () => {
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

  after(() => {
    // Delete the pipeline to clean up
    pipelines.forEach((pipeline) => cy.cleanup_pipelines(headers, pipeline));
  });

  it('Should be able to create a pipeline with splitter transform from scratch', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.url().should('include', '/studio');
    const fileSourceInfo: INodeInfo = { nodeName: 'File', nodeType: 'batchsource' };
    const fileSourceId: INodeIdentifier = { ...fileSourceInfo, nodeId: '0' };
    const csvTransformInfo: INodeInfo = { nodeName: 'CSVParser', nodeType: 'transform' };
    const csvTransformId: INodeIdentifier = { ...csvTransformInfo, nodeId: '1' };
    const jsTransformInfo: INodeInfo = { nodeName: 'JavaScript', nodeType: 'transform' };
    const jsTransformId: INodeIdentifier = { ...jsTransformInfo, nodeId: '2' };
    const unionSplitterTransformInfo: INodeInfo = { nodeName: 'UnionSplitter', nodeType: 'splittertransform' };
    const unionSplitterTransformId: INodeIdentifier = { ...unionSplitterTransformInfo, nodeId: '3' };
    // string branch
    const stringJsTransformInfo: INodeInfo = { nodeName: 'JavaScript', nodeType: 'transform' };
    const stringJsTransformId: INodeIdentifier = { ...stringJsTransformInfo, nodeId: '4' };
    const stringFileSinkInfo: INodeInfo = { nodeName: 'File', nodeType: 'batchsink' };
    const stringFileSinkId: INodeIdentifier = { ...stringFileSinkInfo, nodeId: '5' };
    // int branch
    const intJsTransformInfo: INodeInfo = { nodeName: 'JavaScript', nodeType: 'transform' };
    const intJsTransformId: INodeIdentifier = { ...intJsTransformInfo, nodeId: '6' };
    const intFileSinkInfo: INodeInfo = { nodeName: 'File', nodeType: 'batchsink' };
    const intFileSinkId: INodeIdentifier = { ...intFileSinkInfo, nodeId: '7' };
    const csvJSInputCode = `
    function transform(input, emitter, context) {
      if (input.name === 'Phyllis' || input.name === 'Nicole') {
       emitter.emit({
         name: input.name,
         product: input.product,
         price: parseInt(input.price, 10)
       })
     }
     emitter.emit(input);
   }`;
    const stringJsInputCode = `
    function transform(input, emitter, context) {
      emitter.emit(input);
      emitter.emitError({
        'errorCode': 31,
        'errorMsg': 'Something went wrong',
        'invalidRecord': input
      });
    }`;
    const intJsInputCode = `
    function transform(input, emitter, context) {
      emitter.emit(input);
    }`;
    cy.add_node_to_canvas(fileSourceId);
    cy.open_transform_panel();
    cy.add_node_to_canvas(csvTransformId);
    cy.add_node_to_canvas(jsTransformId);
    cy.add_node_to_canvas(unionSplitterTransformId);
    cy.add_node_to_canvas(stringJsTransformId);
    cy.open_sink_panel();
    cy.add_node_to_canvas(stringFileSinkId);
    cy.open_transform_panel();
    cy.add_node_to_canvas(intJsTransformId);
    cy.open_sink_panel();
    cy.add_node_to_canvas(intFileSinkId);

    cy.pipeline_clean_up_graph_control();
    cy.fit_pipeline_to_screen();
    cy.connect_two_nodes(fileSourceId, csvTransformId, Helpers.getGenericEndpoint);
    cy.connect_two_nodes(csvTransformId, jsTransformId, Helpers.getGenericEndpoint);
    cy.connect_two_nodes(jsTransformId, unionSplitterTransformId, Helpers.getGenericEndpoint);

    cy.open_node_property(fileSourceId);
    cy.get('input[data-cy="referenceName"]').type('File');
    cy.get('input[data-cy="path"]').type('/tmp/cdap-ui-integration-fixtures/purchase_bad.csv');
    cy.close_node_property();

    cy.open_node_property(csvTransformId);
    cy.get('input[data-cy="field"]').type('body');
    cy.get('[data-cy="select-schema-actions-dropdown"] [role="button"]').click();
    cy.get('[data-cy="option-clear"]').click();
    addField(0, 'name');
    addField(1, 'product');
    addField(2, 'price');
    cy.close_node_property();

    cy.open_node_property(jsTransformId);
    cy.window().then((win) => {
      cy.get('.ace-editor-ref').then((aceElement) => {
        const aceEditor = win.ace.edit(aceElement[0]);
        aceEditor.setValue(csvJSInputCode, -1);
      });
    });
    setFieldType(2, 'union');
    cy.get(`[data-cy="schema-row-3"] [data-cy="schema-field-add-button"]`).click();
    setFieldType(4, 'int');
    cy.close_node_property();

    cy.open_node_property(unionSplitterTransformId);
    cy.get('[data-cy="select-unionField"]').click();
    cy.get('[data-cy="option-price"]').click();
    cy.get('[data-cy="get-schema-btn"]').click();
    cy.wait(5000);
    cy.get('[data-cy="tab-head-string"]').contains('string');
    cy.close_node_property();

    cy.connect_two_nodes(
      unionSplitterTransformId,
      stringJsTransformId,
      Helpers.getSplitterNodeEndpoint,
      {
        portName: 'string',
      }
    );

    cy.connect_two_nodes(
      unionSplitterTransformId,
      intJsTransformId,
      Helpers.getSplitterNodeEndpoint,
      {
        portName: 'int',
      }
    );

    cy.open_node_property(stringJsTransformId);
    cy.window().then((win) => {
      cy.get('.ace-editor-ref').then((aceElement) => {
        const aceEditor = win.ace.edit(aceElement[0]);
        aceEditor.setValue(stringJsInputCode, -1);
      });
    });
    cy.close_node_property();

    cy.open_node_property(intJsTransformId);
    cy.window().then((win) => {
      cy.get('.ace-editor-ref').then((aceElement) => {
        const aceEditor = win.ace.edit(aceElement[0]);
        aceEditor.setValue(intJsInputCode, -1);
      });
    });
    cy.close_node_property();

    cy.connect_two_nodes(stringJsTransformId, stringFileSinkId, Helpers.getGenericEndpoint);
    cy.connect_two_nodes(intJsTransformId, intFileSinkId, Helpers.getGenericEndpoint);

    cy.open_node_property(stringFileSinkId);
    cy.get('input[data-cy="path"]').type('/tmp/cdap-ui-integration-fixtures/prices_in_string.txt');
    cy.get('[data-cy="select-format"]').click();
    cy.get('[data-cy="option-csv"]').click();
    cy.get('input[data-cy="referenceName"]').type('StringFileSink');
    cy.close_node_property();

    cy.open_node_property(intFileSinkId);
    cy.get('input[data-cy="referenceName"]').type('IntFileSink');
    cy.get('input[data-cy="path"]').type('/tmp/cdap-ui-integration-fixtures/prices_in_int.txt');
    cy.close_node_property();

    cy.pipeline_clean_up_graph_control();
    cy.fit_pipeline_to_screen();
    // Name pipeline
    cy.get('.pipeline-name').click();
    cy.get('#pipeline-name-input')
      .clear()
      .type(unionSplitterPipeline)
      .type('{enter}');

    // Time for hiding success banner is 3s.
    cy.wait(4000);
    cy.get(Helpers.dataCy('pipeline-preview-btn')).should('be.visible');
  });

  it('Should show preview data for multiple outputs for splitter and correct message for conditional', () => {
    cy.window().then((window) => {
      skipPreviewTests = window.CDAP_CONFIG.hydrator.previewEnabled !== true;
    });
    if (skipPreviewTests) {
      skip();
    }
    cy.get(Helpers.dataCy('pipeline-preview-btn')).click();
    cy.get(Helpers.dataCy('preview-top-run-btn')).click();
    cy.get(Helpers.dataCy('stop-preview-btn')).should('be.visible');
    cy.get(Helpers.dataCy('preview-top-run-btn'), { timeout: 60000 }).should('be.visible');

    // Check number of output fields for null splitter
    cy.get(Helpers.dataCy('UnionSplitter-preview-data-btn')).click();
    cy.get(Helpers.dataCy('toggle-Table')).should('exist');
    cy.get(Helpers.dataCy('tab-head-Int')).should('be.visible');
    cy.get(Helpers.dataCy('tab-head-String')).should('be.visible');
    cy.get('[data-cy="tab-content-Int"] tbody tr').then((intTableRows) => {
      expect(intTableRows).to.have.length(2);
    });
    cy.get(Helpers.dataCy('tab-head-String')).click();
    cy.get('[data-cy="tab-content-String"] tbody tr').then((stringTableRows) => {
      expect(stringTableRows).to.have.length(9);
    });
    cy.get(closeButton).click();
    cy.get(Helpers.dataCy('preview-active-btn')).click();
  });

  it('Should deploy pipeline with union splitter and condition plugins', (done) => {
    cy.get('[data-testid=deploy-pipeline]').click();
    cy.get('[data-cy="Deployed"]', { timeout: 60000 }).should('contain', 'Deployed');
    cy.url()
      .should('include', `/view/${unionSplitterPipeline}`)
      .then(() => done());
  });

  it('Should upload and deploy pipeline with null splitter plugin', (done) => {
    Helpers.deployAndTestPipeline(
      'null_splitter_pipeline-cdap-data-pipeline.json',
      nullSplitterPipeline,
      done
    );
  });
});
