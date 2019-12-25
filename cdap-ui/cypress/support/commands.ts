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

import { ConnectionType } from '../../app/cdap/components/DataPrepConnections/ConnectionType';
import { DEFAULT_GCP_PROJECTID, DEFAULT_GCP_SERVICEACCOUNT_PATH } from '../support/constants';
import { INodeIdentifier, INodeInfo, IgetNodeIDOptions } from '../typings';
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

Cypress.Commands.add('connect_two_nodes', (
  sourceNode: INodeIdentifier,
  targetNode: INodeIdentifier,
  sourceEndpoint: (options: IgetNodeIDOptions, s: string) => string,
  options: IgetNodeIDOptions = {},
) => {
  cy.get_node(sourceNode).then(sourceEl => {
    cy.get_node(targetNode).then(targetEl => {
      let sourceCoOrdinates = sourceEl[0].getBoundingClientRect();
      let targetCoOrdinates = targetEl[0].getBoundingClientRect();
      // connect from source endpoint to midway between the target node
      cy.move_node(
        sourceEndpoint(options, sourceEl[0].id),
        (targetCoOrdinates.left - sourceCoOrdinates.right + (targetCoOrdinates.width / 2)),
        (targetCoOrdinates.top - sourceCoOrdinates.bottom + (targetCoOrdinates.height / 2))
      );
    });
  });
});

Cypress.Commands.add('get_node', (element: INodeIdentifier) => {
  const { nodeName, nodeType, nodeId } = element;
  let elementId = `[data-cy="plugin-node-${nodeName}-${nodeType}-${nodeId}"]`;
  return cy.get(elementId).then(e => cy.wrap(e));
});
