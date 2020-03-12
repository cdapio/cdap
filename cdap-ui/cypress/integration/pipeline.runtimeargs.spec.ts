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
import { dataCy } from '../helpers';

let headers = {};
const PIPELINE_RUN_TIMEOUT = 40000;
const RUNTIME_ARGS_PREVIEW_SELECTOR = 'runtimeargs-preview';
const RUNTIME_ARGS_DEPLOYED_SELECTOR = 'runtimeargs-deployed';
const RUNTIME_ARGS_KEY_SELECTOR = 'runtimeargs-key';
const RUNTIME_ARGS_VALUE_SELECTOR = 'runtimeargs-value';
const PREVIEW_FAILED_BANNER_MSG =
  'The preview of the pipeline "Airport_test_macros" has failed. Please check the logs for more information.';
const PREVIEW_SUCCESS_BANNER_MSG =
  'The preview of the pipeline "Airport_test_macros" has completed successfully.';
const SINK_PATH_VAL = '/tmp/cdap-ui-integration-fixtures';
const SOURCE_PATH_VAL = 'file:/tmp/cdap-ui-integration-fixtures/airports.csv';

/* Disabling preview tests for now because our E2E tests don't have preview enabled
 describe('Creating pipeline with macros ', () => {
  before(() => {
    loginIfRequired().then(() => {
      cy.getCookie('CDAP_Auth_Token').then(cookie => {
        if (!cookie) {
          return;
        }
        headers = {
          Authorization: 'Bearer ' + cookie.value
        };
      });
    });
  });

  beforeEach(() => {
    getArtifactsPoll(headers);
  });

  it('should be successful for preview', () => {
    // Go to Pipelines studio
    cy.visit('/pipelines/ns/default/studio');
    cy.url().should('include', '/studio');
    cy.upload_pipeline(
      'pipeline_with_macros.json',
      '#pipeline-import-config-link > input[type="file"]'
    ).then(subject => {
      expect(subject.length).to.be.eq(1);
    });
  });

  it('and providing wrong runtime arguments should fail the pipeline preview.', () => {
    cy.get(dataCy('pipeline-preview-btn')).click();
    cy.get(dataCy('preview-config-btn')).click();
    cy.get(dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)).should('exist');
    // Entering fake runtime arguments to fail the pipeline run
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(0)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('be.disabled');
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(0)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('have.value', 'source_path');
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(0)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )} input`
    ).should('have.value', '');
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(0)} ${dataCy(RUNTIME_ARGS_VALUE_SELECTOR)}`
    ).type('random value1');
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(1)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('be.disabled');
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(1)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('have.value', 'sink_path');
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(1)} ${dataCy(RUNTIME_ARGS_VALUE_SELECTOR)}`
    ).should('have.value', '');
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(1)} ${dataCy(RUNTIME_ARGS_VALUE_SELECTOR)}`
    ).type('random value2');
    // Adding new row of runtime arguments
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(2)} ${dataCy(RUNTIME_ARGS_KEY_SELECTOR)}`
    ).should('not.exist');
    cy.get(`${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(1)} ${dataCy('add-row')}`).click();
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(2)} ${dataCy(RUNTIME_ARGS_KEY_SELECTOR)}`
    ).should('exist');
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(2)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('have.value', '');
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(2)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).type('test key 2');
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(2)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('have.value', 'test key 2');
    cy.get(`${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(2)} ${dataCy('remove-row')}`).click();
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(2)} ${dataCy(RUNTIME_ARGS_KEY_SELECTOR)}`
    ).should('not.exist');
    // running pipeline with fake runtime arguments
    cy.get(dataCy('run-preview-btn')).click();
    cy.get(`${dataCy('valium-banner-hydrator')} span`).should(
      'have.text',
      PREVIEW_FAILED_BANNER_MSG
    );
    cy.get(`${dataCy('valium-banner-hydrator')} button`).click();
  });

  it('and providing right runtime arguments should pass the pipeline preview.', () => {
    cy.get(dataCy('preview-config-btn')).click();
    cy.get(dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)).should('exist');
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(0)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('be.disabled');
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(0)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('have.value', 'source_path');
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(0)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )} input`
    ).should('have.value', 'random value1');
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(0)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )} input`
    ).clear();
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(0)} ${dataCy(RUNTIME_ARGS_VALUE_SELECTOR)}`
    ).type(SOURCE_PATH_VAL);

    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(1)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('be.disabled');
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(1)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('have.value', 'sink_path');
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(1)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )} input`
    ).clear();
    cy.get(
      `${dataCy(RUNTIME_ARGS_PREVIEW_SELECTOR)} ${dataCy(1)} ${dataCy(RUNTIME_ARGS_VALUE_SELECTOR)}`
    ).type(SINK_PATH_VAL);
    cy.get(dataCy('run-preview-btn')).click();
    cy.get(`${dataCy('valium-banner-hydrator')} span`).should(
      'have.text',
      PREVIEW_SUCCESS_BANNER_MSG
    );
    cy.get(`${dataCy('valium-banner-hydrator')} button`).click();
  });
});
 */
describe('Deploying pipeline with temporary runtime arguments', () => {
  const runtimeArgsPipeline = `runtime_args_pipeline_${Date.now()}`;
  before(() => {
    loginIfRequired().then(() => {
      cy.getCookie('CDAP_Auth_Token').then(cookie => {
        if (!cookie) {
          return;
        }
        headers = {
          Authorization: 'Bearer ' + cookie.value
        };
      });
    });
  });

  beforeEach(() => {
    getArtifactsPoll(headers);
  });
  after(() => {
    cy.cleanup_pipelines(headers, runtimeArgsPipeline);
  });

  it('should be successful', () => {
    // Go to Pipelines studio
    cy.visit('/pipelines/ns/default/studio');
    cy.url().should('include', '/studio');
    cy.upload_pipeline(
      'pipeline_with_macros.json',
      '#pipeline-import-config-link > input[type="file"]'
    ).then(subject => {
      expect(subject.length).to.be.eq(1);
    });
  });
  it('and running it should succeed.', () => {
    cy.get('.pipeline-name').click();
    cy.get('#pipeline-name-input')
      .clear()
      .type(runtimeArgsPipeline)
      .type('{enter}');
    cy.get(dataCy('deploy-pipeline-btn')).click();
    cy.get(dataCy('Deployed')).should('exist');
    cy.get(dataCy('pipeline-run-btn')).click();
    cy.get(dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)).should('exist');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(0)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('be.disabled');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(0)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('have.value', 'source_path');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(0)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )} input`
    ).should('have.value', '');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(0)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )}`
    ).type('random value1');

    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(1)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('be.disabled');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(1)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('have.value', 'sink_path');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(1)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )} input`
    ).should('have.value', '');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(1)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )}`
    ).type('random value2');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(2)} ${dataCy(RUNTIME_ARGS_KEY_SELECTOR)}`
    ).should('not.exist');
    cy.get(`${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(1)} ${dataCy('add-row')}`).click();
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(2)} ${dataCy(RUNTIME_ARGS_KEY_SELECTOR)}`
    ).should('exist');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(2)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('have.value', '');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(2)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).type('test key 2');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(2)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('have.value', 'test key 2');

    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(2)} ${dataCy('remove-row')}`
    ).click();
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(2)} ${dataCy(RUNTIME_ARGS_KEY_SELECTOR)}`
    ).should('not.exist');

    cy.get('[datacy="run-deployed-pipeline-modal-btn"]').click();
    cy.get(dataCy('Failed'), { timeout: PIPELINE_RUN_TIMEOUT }).should('exist');

    cy.get(dataCy('pipeline-run-btn')).click();
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(0)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )} input`
    ).clear();
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(0)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )}`
    ).type(SOURCE_PATH_VAL);
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(1)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )} input`
    ).clear();
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(1)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )}`
    ).type(SINK_PATH_VAL);
    cy.get('[datacy="run-deployed-pipeline-modal-btn"]').click();
    cy.get(dataCy('Succeeded'), { timeout: PIPELINE_RUN_TIMEOUT }).should('exist');
  });
});

describe('Deploying pipeline with saved runtime arguments', () => {
  const runtimeArgsPipeline = `runtime_args_pipeline_${Date.now()}`;
  before(() => {
    loginIfRequired().then(() => {
      cy.getCookie('CDAP_Auth_Token').then(cookie => {
        if (!cookie) {
          return;
        }
        headers = {
          Authorization: 'Bearer ' + cookie.value
        };
      });
    });
  });

  beforeEach(() => {
    getArtifactsPoll(headers);
  });
  after(() => {
    // cy.cleanup_pipelines(headers, runtimeArgsPipeline);
  });

  it('should be successful', () => {
    // Go to Pipelines studio
    cy.visit('/pipelines/ns/default/studio');
    cy.url().should('include', '/studio');
    cy.upload_pipeline(
      'pipeline_with_macros.json',
      '#pipeline-import-config-link > input[type="file"]'
    ).then(subject => {
      expect(subject.length).to.be.eq(1);
    });
  });

  it('and running it should succeed.', () => {
    cy.get('.pipeline-name').click();
    cy.get('#pipeline-name-input')
      .clear()
      .type(runtimeArgsPipeline)
      .type('{enter}');
    cy.get(dataCy('deploy-pipeline-btn')).click();
    cy.get(dataCy('Deployed')).should('exist');
    cy.get(dataCy('pipeline-run-btn')).click();
    cy.get(dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)).should('exist');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(0)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('be.disabled');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(0)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('have.value', 'source_path');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(0)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )} input`
    ).should('have.value', '');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(0)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )}`
    ).type(SOURCE_PATH_VAL);

    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(1)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('be.disabled');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(1)} ${dataCy(
        RUNTIME_ARGS_KEY_SELECTOR
      )} input`
    ).should('have.value', 'sink_path');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(1)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )} input`
    ).should('have.value', '');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(1)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )}`
    ).type(SINK_PATH_VAL);
    cy.get('[datacy="run-deployed-pipeline-modal-btn"]').click();
    cy.get(dataCy('pipeline-run-btn')).click();
    cy.get(dataCy('Succeeded'), { timeout: PIPELINE_RUN_TIMEOUT }).should('exist');
  });
});
