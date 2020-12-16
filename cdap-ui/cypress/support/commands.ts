/*
 * Copyright © 2019 Cask Data, Inc.
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

import {
  DEFAULT_GCP_PROJECTID,
  DEFAULT_GCP_SERVICEACCOUNT_PATH,
  RUNTIME_ARGS_DEPLOYED_SELECTOR,
  RUNTIME_ARGS_KEY_SELECTOR,
  RUNTIME_ARGS_VALUE_SELECTOR,
} from '../support/constants';
import { INodeIdentifier, INodeInfo, IgetNodeIDOptions } from '../typings';
import {
  dataCy,
  getConditionNodeEndpoint,
  getGenericEndpoint,
  getNodeSelectorFromNodeIndentifier,
} from '../helpers';

import { ConnectionType } from '../../app/cdap/components/DataPrepConnections/ConnectionType';

/**
 * Uploads a pipeline json from fixtures to input file element.
 *
 * @fileName - Name of the file from fixture folder including extension
 * @selector - css selector to query for the input[type="file"] element.
 */
Cypress.Commands.add('upload_pipeline', (fileName, selector) => {
  return cy.get(selector, { timeout: 60000 }).then((subject) => {
    return cy.fixture(fileName).then((pipeline1) => {
      const el = subject[0];
      const blob = new Blob([JSON.stringify(pipeline1, null, 2)], { type: 'application/json' });
      return cy.window().then((win) => {
        const testFile = new win.File([blob], fileName, {
          type: 'application/json',
        });
        const dataTransfer = new win.DataTransfer();
        dataTransfer.items.add(testFile);
        el.files = dataTransfer.files;
        return cy.wrap(subject).trigger('change', { force: true });
      });
    });
  });
});

Cypress.Commands.add(
  'upload',
  {
    prevSubject: 'element',
  },
  (subject, file, fileName, fileType) => {
    cy.window().then((window) => {
      const blob = new Blob([file], { type: fileType });
      const testFile = new window.File([blob], fileName);
      const dataTransfer = new window.DataTransfer();
      dataTransfer.items.add(testFile);
      cy.wrap(subject).trigger('drop', {
        dataTransfer,
      });
    });
  }
);

Cypress.Commands.add('upload_draft_via_api', (headers, pipelineJson) => {
  return cy.request({
    method: 'PUT',
    url: `http://${Cypress.env('host')}:11015/v3/configuration/user`,
    headers,
    body: pipelineJson
  }).then(resp => {
    expect(resp.status).to.be.eq(200);
  });
});

Cypress.Commands.add('cleanup_pipelines', (headers, pipelineName) => {
  return cy
    .request({
      method: 'GET',
      url: `http://${Cypress.env('host')}:11015/v3/namespaces/default/apps/${pipelineName}`,
      failOnStatusCode: false,
      headers,
    })
    .then((response) => {
      if (response.status === 200) {
        return cy.request({
          method: 'DELETE',
          url: `http://${Cypress.env('host')}:11015/v3/namespaces/default/apps/${pipelineName}`,
          failOnStatusCode: false,
          headers,
        });
      }
    });
});

Cypress.Commands.add(
  'fill_GCS_connection_create_form',
  (
    connectionId,
    projectId = DEFAULT_GCP_PROJECTID,
    serviceAccountPath = DEFAULT_GCP_SERVICEACCOUNT_PATH
  ) => {
    cy.visit('/cdap/ns/default/connections');
    cy.get('[data-cy="wrangler-add-connection-button"]', { timeout: 60000 }).click();
    cy.get(`[data-cy="wrangler-connection-${ConnectionType.GCS}`).click();
    cy.get(`[data-cy="wrangler-${ConnectionType.GCS}-connection-name"]`).type(connectionId);
    cy.get(`[data-cy="wrangler-${ConnectionType.GCS}-connection-projectid"]`).type(projectId);
    cy.get(`[data-cy="wrangler-${ConnectionType.GCS}-connection-serviceaccount-filepath"]`).type(
      serviceAccountPath
    );
  }
);

Cypress.Commands.add('create_GCS_connection', (connectionId) => {
  cy.fill_GCS_connection_create_form(connectionId);
  cy.get(`[data-cy="wrangler-${ConnectionType.GCS}-add-connection-button"]`).click({
    timeout: 60000,
  });
});

Cypress.Commands.add('test_GCS_connection', (connectionId, projectId, serviceAccountPath) => {
  cy.fill_GCS_connection_create_form(connectionId, projectId, serviceAccountPath);
  cy.get(`[data-cy="wrangler-${ConnectionType.GCS}-test-connection-button"]`).click({
    timeout: 60000,
  });
});

Cypress.Commands.add(
  'fill_BIGQUERY_connection_create_form',
  (
    connectionId,
    projectId = DEFAULT_GCP_PROJECTID,
    serviceAccountPath = DEFAULT_GCP_SERVICEACCOUNT_PATH
  ) => {
    cy.visit('/cdap/ns/default/connections');
    cy.get('[data-cy="wrangler-add-connection-button"]', { timeout: 30000 }).click();
    cy.get(`[data-cy="wrangler-connection-${ConnectionType.BIGQUERY}`).click();
    cy.get(`[data-cy="wrangler-${ConnectionType.BIGQUERY}-connection-name"]`).type(connectionId);
    cy.get(`[data-cy="wrangler-${ConnectionType.BIGQUERY}-connection-projectid"]`).type(projectId);
    cy.get(
      `[data-cy="wrangler-${ConnectionType.BIGQUERY}-connection-serviceaccount-filepath"]`
    ).type(serviceAccountPath);
  }
);

Cypress.Commands.add('create_BIGQUERY_connection', (connectionId) => {
  cy.fill_BIGQUERY_connection_create_form(connectionId);
  cy.get(`[data-cy="wrangler-${ConnectionType.BIGQUERY}-add-connection-button"]`).click({
    timeout: 60000,
  });
});

Cypress.Commands.add('test_BIGQUERY_connection', (connectionId, projectId, serviceAccountPath) => {
  cy.fill_BIGQUERY_connection_create_form(connectionId, projectId, serviceAccountPath);
  cy.get(`[data-cy="wrangler-${ConnectionType.BIGQUERY}-test-connection-button"]`).click({
    timeout: 60000,
  });
});

Cypress.Commands.add(
  'fill_SPANNER_connection_create_form',
  (
    connectionId,
    projectId = DEFAULT_GCP_PROJECTID,
    serviceAccountPath = DEFAULT_GCP_SERVICEACCOUNT_PATH
  ) => {
    cy.visit('/cdap/ns/default/connections');
    cy.get('[data-cy="wrangler-add-connection-button"]', { timeout: 30000 }).click();
    cy.get(`[data-cy="wrangler-connection-${ConnectionType.SPANNER}`).click();
    cy.get(`[data-cy="wrangler-${ConnectionType.SPANNER}-connection-name"]`).type(connectionId);
    cy.get(`[data-cy="wrangler-${ConnectionType.SPANNER}-connection-projectid"]`).type(projectId);
    cy.get(
      `[data-cy="wrangler-${ConnectionType.SPANNER}-connection-serviceaccount-filepath"]`
    ).type(serviceAccountPath);
  }
);

Cypress.Commands.add('create_SPANNER_connection', (connectionId) => {
  cy.fill_SPANNER_connection_create_form(connectionId);
  cy.get(`[data-cy="wrangler-${ConnectionType.SPANNER}-add-connection-button"]`).click({
    timeout: 60000,
  });
});

Cypress.Commands.add('test_SPANNER_connection', (connectionId, projectId, serviceAccountPath) => {
  cy.fill_SPANNER_connection_create_form(connectionId, projectId, serviceAccountPath);
  cy.get(`[data-cy="wrangler-${ConnectionType.SPANNER}-test-connection-button"]`).click({
    timeout: 60000,
  });
});

let wranglerStartIteration = 1;
Cypress.Commands.add('start_wrangler', (headers) => {
  cy.request({
    url: `http://${Cypress.env(
      'host'
    )}:11015/v3/namespaces/system/apps/dataprep/services/service/status`,
    failOnStatusCode: false,
    headers,
  }).then((response) => {
    if (response.status === 404) {
      // This means wrangler as application is not there.
      cy.log('Unable to find wrangler artifact. No wrangler application avaiable to test');
      cy.request({
        url: `http://${Cypress.env('host')}:11015/v3/namespaces/default/artifacts`,
        failOnStatusCode: false,
        headers,
      }).then((artifactsResponse) => {
        if (artifactsResponse.status !== 200) {
          cy.log('Unable to find wrangler artifact.');
          return;
        }
        if (artifactsResponse.body) {
          let artifacts = artifactsResponse.body;
          if (typeof artifactsResponse.body === 'string') {
            try {
              artifacts = JSON.parse(artifactsResponse.body);
            } catch (e) {
              cy.log('Unable to find wrangler artifact. ', e);
              return;
            }
          }
          const wranglerArtifact = artifacts.find((artifact) => {
            return artifact.name === 'wrangler-service';
          });
          if (wranglerArtifact) {
            cy.request({
              url: `http://${Cypress.env(
                'host'
              )}:11015/v3/namespaces/system/apps/dataprep?namespace=default`,
              failOnStatusCode: false,
              headers,
              method: 'PUT',
              body: {
                artifact: wranglerArtifact,
              },
            }).then((wranglerAppCreateResponse) => {
              if (wranglerAppCreateResponse.status === 200) {
                cy.wait(20000);
                cy.start_wrangler(headers);
              }
            });
          }
        }
      });
      return cy.wrap(null);
    }
    let resBody = response.body;
    if (resBody) {
      if (typeof resBody === 'string') {
        try {
          resBody = JSON.parse(resBody);
        } catch (e) {
          cy.log('Got an invalid JSON response for wrangler service status: ', e, response.body);
          return cy.wrap(null);
        }
      }

      if (typeof resBody === 'object') {
        if (resBody.status === 'STARTING') {
          return cy.wait(10000).then(() => {
            if (wranglerStartIteration > 10) {
              throw new Error('Too many attempts(10) to start wrangler failed. Aborting');
            }
            cy.log(
              `Wrangler STARTING. Attempt(${wranglerStartIteration++}) to check status and navigate`
            );
            cy.start_wrangler(headers);
          });
        }
        if (resBody.status === 'RUNNING') {
          wranglerStartIteration = 1;
          cy.wait(20000);
          return cy.wrap(response.body.status);
        }
        if (resBody.status === 'STOPPED') {
          return cy
            .request({
              url: `http://${Cypress.env(
                'host'
              )}:11015/v3/namespaces/system/apps/dataprep/services/service/start`,
              failOnStatusCode: false,
              headers,
              method: 'POST',
            })
            .then((resp) => {
              if (resp.status === 200) {
                cy.wait(10000).then(() => {
                  if (wranglerStartIteration > 10) {
                    throw new Error('Too many attempts(10) to start wrangler failed. Aborting');
                  }
                  cy.log(
                    `Wrangler STARTING. Attempt(${wranglerStartIteration++}) to check status and navigate`
                  );
                  cy.start_wrangler(headers);
                });
              }
            });
        }
      }
    }
  });
});

Cypress.Commands.add('open_source_panel', () => {
  cy.get('[data-cy="plugin-Source-group"]').click();
});
Cypress.Commands.add('open_transform_panel', () => {
  cy.get('[data-cy="plugin-Transform-group"]').click();
});
Cypress.Commands.add('open_analytics_panel', () => {
  cy.get('[data-cy="plugin-Analytics-group"]').click();
});
Cypress.Commands.add('open_sink_panel', () => {
  cy.get('[data-cy="plugin-Sink-group"]').click();
});
Cypress.Commands.add('open_condition_and_actions_panel', () => {
  cy.get('[data-cy="plugin-Conditions and Actions-group"]').click();
});

Cypress.Commands.add('add_node_to_canvas', (nodeObj: INodeInfo) => {
  const { nodeName, nodeType } = nodeObj;
  return cy.get(`[data-cy="plugin-${nodeName}-${nodeType}"]`).click();
});

Cypress.Commands.add('move_node', (node: INodeIdentifier | string, toX: number, toY: number) => {
  let nodeSelector;
  if (typeof node === 'object') {
    const { nodeName, nodeType, nodeId } = node;
    nodeSelector = `[data-cy="plugin-node-${nodeName}-${nodeType}-${nodeId}"]`;
  } else {
    nodeSelector = node;
  }
  cy.get(nodeSelector)
    .trigger('mousedown', { which: 1, pageX: 0, pageY: 0 })
    .trigger('mousemove', { which: 1, pageX: toX, pageY: toY })
    .trigger('mouseup', { force: true });
});

Cypress.Commands.add('select_from_to', (from: INodeIdentifier, to: INodeIdentifier) => {
  let fromNodeElement;
  let toNodeElement;
  // Make sure nodes are aligned  so that correct nodes are selected
  cy.get('[data-cy="pipeline-clean-up-graph-control"]').click();
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
      cy.get('#diagram-container').trigger('mousedown', {
        which: 1,
        force: true,
        clientX: fromX - 10,
        clientY: fromY - 10,
      });
      cy.get('#diagram-container')
        .trigger('mousemove', {
          which: 1,
          clientX: toX + toWidth + 10,
          clientY: toY + toHeight + 10,
        })
        .trigger('mouseup', { force: true })
        .trigger('click', { force: true });
    });
  });
});

Cypress.Commands.add('select_connection', (from: INodeIdentifier, to: INodeIdentifier) => {
  let fromNodeElement;
  let toNodeElement;
  cy.get_node(from).then((sElement) => {
    fromNodeElement = sElement;
    cy.get_node(to).then((tElement) => {
      toNodeElement = tElement;
      const sourceName = fromNodeElement[0].getAttribute('id');
      const targetName = toNodeElement[0].getAttribute('id');
      const connectionSelector = `.jsplumb-connector.connection-id-endpoint_${sourceName}-${targetName}`;
      cy.get(connectionSelector).then((connElement) => {
        (connElement[0] as any)._jsPlumb._jsPlumb.instance.fire(
          'click',
          (connElement[0] as any)._jsPlumb
        );
        return cy.wrap(connElement[0]);
      });
    });
  });
});

Cypress.Commands.add(
  'connect_two_nodes',
  (
    sourceNode: INodeIdentifier,
    targetNode: INodeIdentifier,
    sourceEndpoint: (options: IgetNodeIDOptions, s: string) => string,
    options: IgetNodeIDOptions = {}
  ) => {
    cy.get_node(sourceNode).then((sourceEl) => {
      cy.get_node(targetNode).then((targetEl) => {
        const sourceCoOrdinates = sourceEl[0].getBoundingClientRect();
        const targetCoOrdinates = targetEl[0].getBoundingClientRect();
        let pageX = targetCoOrdinates.left - sourceCoOrdinates.right + targetCoOrdinates.width / 2;
        let pageY = targetCoOrdinates.top - sourceCoOrdinates.bottom + targetCoOrdinates.height / 2;
        // This is not perfect. Since splitter endpoints are on a popover
        // we need to go extra down and left to connect from the endpoint
        // to the target node.
        if (options.portName) {
          pageX -= targetCoOrdinates.width / 2;
          pageY += targetCoOrdinates.height / 2;
        }
        // connect from source endpoint to midway between the target node
        cy.move_node(sourceEndpoint(options, sourceEl[0].id), pageX, pageY);
      });
    });
  }
);

Cypress.Commands.add('get_node', (element: INodeIdentifier) => {
  const elementId = getNodeSelectorFromNodeIndentifier(element);
  return cy.get(elementId).then((e) => cy.wrap(e));
});

Cypress.Commands.add('create_simple_pipeline', () => {
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

  cy.connect_two_nodes(sourceNodeId, transformNodeId, getGenericEndpoint);
  cy.connect_two_nodes(transformNodeId, sinkNodeId, getGenericEndpoint);
  return cy.wrap({ sourceNodeId, transformNodeId, sinkNodeId });
});

Cypress.Commands.add('create_complex_pipeline', () => {
  // Two BigQuery sources
  const sourceNode1: INodeInfo = { nodeName: 'BigQueryTable', nodeType: 'batchsource' };
  const sourceNodeId1: INodeIdentifier = { ...sourceNode1, nodeId: '0' };
  const sourceNode2: INodeInfo = { nodeName: 'BigQueryTable', nodeType: 'batchsource' };
  const sourceNodeId2: INodeIdentifier = { ...sourceNode2, nodeId: '1' };

  // Two javascript transforms
  const transformNode1: INodeInfo = { nodeName: 'JavaScript', nodeType: 'transform' };
  const transformNodeId1: INodeIdentifier = { ...transformNode1, nodeId: '2' };
  const transformNode2: INodeInfo = { nodeName: 'JavaScript', nodeType: 'transform' };
  const transformNodeId2: INodeIdentifier = { ...transformNode2, nodeId: '3' };

  // One joiner
  const joinerNode: INodeInfo = { nodeName: 'Joiner', nodeType: 'batchjoiner' };
  const joinerNodeId: INodeIdentifier = { ...joinerNode, nodeId: '4' };

  // One condition node
  const conditionNode: INodeInfo = { nodeName: 'Conditional', nodeType: 'condition' };
  const conditionNodeId: INodeIdentifier = { ...conditionNode, nodeId: '5' };

  // Two BigQuery sinks
  const sinkNode1: INodeInfo = { nodeName: 'BigQueryMultiTable', nodeType: 'batchsink' };
  const sinkNodeId1: INodeIdentifier = { ...sinkNode1, nodeId: '6' };
  const sinkNode2: INodeInfo = { nodeName: 'BigQueryMultiTable', nodeType: 'batchsink' };
  const sinkNodeId2: INodeIdentifier = { ...sinkNode2, nodeId: '7' };

  cy.add_node_to_canvas(sourceNode1);
  cy.add_node_to_canvas(sourceNode2);

  cy.open_transform_panel();
  cy.add_node_to_canvas(transformNode1);
  cy.add_node_to_canvas(transformNode2);

  cy.open_analytics_panel();
  cy.add_node_to_canvas(joinerNode);

  cy.open_condition_and_actions_panel();
  cy.add_node_to_canvas(conditionNode);

  cy.open_sink_panel();
  cy.add_node_to_canvas(sinkNode1);
  cy.add_node_to_canvas(sinkNode2);

  cy.get('[data-cy="pipeline-clean-up-graph-control"]').click();
  cy.get('[data-cy="pipeline-fit-to-screen-control"]').click();

  cy.connect_two_nodes(sourceNodeId1, transformNodeId1, getGenericEndpoint);
  cy.connect_two_nodes(sourceNodeId2, transformNodeId2, getGenericEndpoint);

  cy.connect_two_nodes(transformNodeId1, joinerNodeId, getGenericEndpoint);
  cy.connect_two_nodes(transformNodeId2, joinerNodeId, getGenericEndpoint);

  cy.connect_two_nodes(joinerNodeId, conditionNodeId, getGenericEndpoint);

  cy.connect_two_nodes(conditionNodeId, sinkNodeId1, getConditionNodeEndpoint, { condition: true });
  cy.connect_two_nodes(conditionNodeId, sinkNodeId2, getConditionNodeEndpoint, {
    condition: false,
  });

  cy.get('[data-cy="pipeline-clean-up-graph-control"]').click();
  cy.get('[data-cy="pipeline-fit-to-screen-control"]').click();

  return cy.wrap({
    sourceNodeId1,
    sourceNodeId2,
    transformNodeId1,
    transformNodeId2,
    joinerNodeId,
    conditionNodeId,
    sinkNodeId1,
    sinkNodeId2,
  });
});

Cypress.Commands.add('get_pipeline_json', () => {
  cy.get('[data-cy="pipeline-export-btn"]').click();

  cy.get('textarea[data-cy="pipeline-export-json-container"]')
    .invoke('val')
    .then((va) => {
      if (typeof va !== 'string') {
        throw new Error('Unable to get pipeline config');
      }
      let pipelineConfig;
      try {
        pipelineConfig = JSON.parse(va);
      } catch (e) {
        throw new Error('Invalid pipeline config');
      }
      cy.get('[data-cy="export-pipeline-close-modal-btn"]').click();
      return cy.wrap(pipelineConfig);
    });
});

Cypress.Commands.add('get_pipeline_stage_json', (stageName: string) => {
  cy.get_pipeline_json().then((pipelineConfig) => {
    const stages = pipelineConfig.config.stages;
    const stageInfo = stages.find((stage) => stage.name === stageName);

    return cy.wrap(stageInfo);
  });
});

Cypress.Commands.add('open_node_property', (element: INodeIdentifier) => {
  const { nodeName, nodeType, nodeId } = element;
  const elementId = `[data-cy="plugin-node-${nodeName}-${nodeType}-${nodeId}"]`;
  cy.get(`${elementId} .node .node-metadata .node-version`).invoke('hide');
  cy.get(`${elementId} .node .node-configure-btn`)
    .invoke('show')
    .click();
});

Cypress.Commands.add('close_node_property', () => {
  cy.get('[data-testid="close-config-popover"]').click();
});

/**
 * row - row index for the runtime argument key value pair.
 */
Cypress.Commands.add(
  'add_runtime_args_row_with_value',
  (row: number, key: string, value: string) => {
    // clicking add on previous row.
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(row - 1)} ${dataCy('add-row')}`
    ).click();
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(row)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )}`
    ).should('exist');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(row)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).type(key);
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(row)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )} input`
    ).type(value);
  }
);

/**
 * row - row index for the runtime argument key value pair.
 */
Cypress.Commands.add(
  'update_runtime_args_row',
  (row: number, key: string, value: string, macro: boolean = false) => {
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(row)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )}`
    ).should('exist');
    if (!macro) {
      cy.get(
        `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(row)} ${dataCy(
          RUNTIME_ARGS_KEY_SELECTOR
        )} input`
      ).clear();
      cy.get(
        `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(row)} ${dataCy(
          RUNTIME_ARGS_KEY_SELECTOR
        )}`
      ).type(key);
    }
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(row)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )} input`
    ).should('exist');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(row)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )} input`
    ).clear();
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(row)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )}`
    ).type(value);
  }
);

/**
 * row - row index for the runtime argument key value pair.
 */
Cypress.Commands.add(
  'assert_runtime_args_row',
  (row: number, key: string, value: string, macro: boolean = false) => {
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(row)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )}`
    ).should('exist');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(row)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('have.value', key);
    if (macro) {
      cy.get(
        `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(row)} ${dataCy(
          RUNTIME_ARGS_KEY_SELECTOR
        )} input`
      ).should('be.disabled');
    }
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(row)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )}`
    ).should('exist');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(row)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )} input`
    ).should('have.value', value);
  }
);

/**
 * Uploads a plugin json from fixtures to input file element.
 *
 * @fileName - Name of the file from fixture folder including extension
 * @selector - data-cy selector to query for the input[type="file"] element.
 */
Cypress.Commands.add('upload_plugin_json', (fileName, selector) => {
  return cy.get(dataCy(selector), { timeout: 60000 }).then((subject) => {
    return cy.fixture(fileName).then((pluginJSON) => {
      const el = subject[0];
      const JSONContent =
        typeof pluginJSON === 'string' ? pluginJSON : JSON.stringify(pluginJSON, undefined, 2);
      const blob = new Blob([JSONContent], { type: 'application/json' });
      return cy.window().then((win) => {
        const testFile = new win.File([blob], fileName, {
          type: 'application/json',
        });
        const dataTransfer = new win.DataTransfer();
        dataTransfer.items.add(testFile);
        el.files = dataTransfer.files;
        return cy.wrap(subject).trigger('change', { force: true });
      });
    });
  });
});

/**
 * Cleans up secure keys after executing the tests. This is to remove state
 *  from individual tests.
 *
 * @headers - Any request headers to be passed.
 * @secureKeyName - name of the secure key to be deleted.
 */
Cypress.Commands.add('cleanup_secure_key', (headers, secureKeyName) => {
  return cy.request({
    method: 'DELETE',
    url: `http://${Cypress.env('host')}:11015/v3/namespaces/default/securekeys/${secureKeyName}`,
    failOnStatusCode: false,
    headers,
  });
});

Cypress.Commands.add('delete_artifact_via_api', (headers, artifactName, version) => {
  return cy.request({
    method: 'DELETE',
    url: `http://${Cypress.env('host')}:11015/v3/namespaces/default/artifacts/${artifactName}/versions/${version}`,
    headers,
    failOnStatusCode: false
  }).then(resp => {
    // 404 if the artifact is already deleted.
    expect(resp.status).to.be.oneOf([200, 404]);
  });
});

Cypress.Commands.add('fit_pipeline_to_screen', () => {
  return cy.get('[data-cy="pipeline-fit-to-screen-control"]').click();
});

Cypress.Commands.add('pipeline_clean_up_graph_control', () => {
  return cy.get('[data-cy="pipeline-clean-up-graph-control"]').click();
});
