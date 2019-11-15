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
let headers = {};
describe('Pipeline Studio', () => {
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

  beforeEach(() => {
    Helpers.getArtifactsPoll(headers);
  });

  it('Should renders pipeline studio correctly', () => {
    cy.visit('/pipelines/ns/default/studio');
  });

  it('Should render different types of nodes to the canvas', () => {
    const sourceNode: INodeInfo = { nodeName: 'BigQueryTable', nodeType: 'batchsource' };
    const sourceNodeId: INodeIdentifier = { ...sourceNode, nodeId: '0' };
    const transformNode: INodeInfo = { nodeName: 'Wrangler', nodeType: 'transform' };
    const transformNodeId: INodeIdentifier = { ...transformNode, nodeId: '1' };
    const sinkNode: INodeInfo = { nodeName: 'BigQueryMultiTable', nodeType: 'batchsink' };
    const sinkNodeId: INodeIdentifier = { ...sinkNode, nodeId: '2' };
    cy.add_node_to_canvas(sourceNode);

    cy.open_transform_panel();
    cy.add_node_to_canvas(transformNode);

    cy.open_sink_panel();
    cy.add_node_to_canvas(sinkNode);

    cy.get('#diagram-container');
    cy.move_node(sourceNodeId, 50, 50);
    cy.move_node(transformNodeId, 100, 50);
    cy.move_node(sinkNodeId, 150, 50);
  });
  it('Should be able to connect two nodes in the canvas', () => {
    const sourceNode: INodeInfo = { nodeName: 'BigQueryTable', nodeType: 'batchsource' };
    const sourceNodeId: INodeIdentifier = { ...sourceNode, nodeId: '0' };
    const transformNode: INodeInfo = { nodeName: 'Wrangler', nodeType: 'transform' };
    const transformNodeId: INodeIdentifier = { ...transformNode, nodeId: '1' };
    const sinkNode: INodeInfo = { nodeName: 'BigQueryMultiTable', nodeType: 'batchsink' };
    const sinkNodeId: INodeIdentifier = { ...sinkNode, nodeId: '2' };

    cy.get('#diagram-container');
    let getGenericEndpoint = (id) => `.plugin-endpoint_${id}-right`;
    cy.connect_two_nodes(sourceNodeId, transformNodeId, getGenericEndpoint);
    cy.connect_two_nodes(transformNodeId, sinkNodeId, getGenericEndpoint);
  });
});
