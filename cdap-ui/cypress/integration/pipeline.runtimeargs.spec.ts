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
import {
  RUNTIME_ARGS_DEPLOYED_SELECTOR,
  RUNTIME_ARGS_KEY_SELECTOR,
  RUNTIME_ARGS_VALUE_SELECTOR,
} from '../support/constants';
let headers = {};
const PIPELINE_RUN_TIMEOUT = 360000;
const RUNTIME_ARGS_PREVIEW_SELECTOR = 'runtimeargs-preview';

const RUNTIME_ARGS_MODELESS_LOADING_SELECTOR = 'runtime-args-modeless-loading';
const PREVIEW_SUCCESS_BANNER_MSG =
  'The preview of the pipeline "Airport_test_macros" has completed successfully.';
const SINK_PATH_VAL = '/tmp/cdap-ui-integration-fixtures';
const SOURCE_PATH_VAL = 'file:/tmp/cdap-ui-integration-fixtures/airports.csv';

// We don't have preview enabled in distributed mode.
// TODO(CDAP-16620) To enable preview in distributed mode. Once it is enabled we can remove.
let skipPreviewTests = false;
// @ts-ignore "cy.state" is not in the "cy" type
const getMochaContext = () => cy.state('runnable').ctx;
const skip = () => {
  const ctx = getMochaContext();
  return ctx.skip();
};

describe('Creating pipeline with macros ', () => {
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

  beforeEach(() => {
    getArtifactsPoll(headers);
  });

  it('and providing wrong runtime arguments should fail the pipeline preview.', () => {
    // Go to Pipelines studio
    cy.visit('/pipelines/ns/default/studio');
    cy.window().then((window) => {
      skipPreviewTests = window.CDAP_CONFIG.hydrator.previewEnabled !== true;
    });
    if (skipPreviewTests) {
      skip();
    }
    cy.upload_pipeline(
      'pipeline_with_macros.json',
      '#pipeline-import-config-link > input[type="file"]'
    );
    cy.wait(10000);
    cy.get(dataCy('pipeline-preview-btn')).click();
    cy.get(dataCy('preview-top-run-btn')).click();
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
    // running pipeline with fake runtime arguments should fail with error banner
    cy.get(dataCy('preview-configure-run-btn')).click();
    cy.get(`${dataCy('valium-banner-hydrator')}`).within(() => {
      cy.get('.alert-danger').should('be.visible');
    });
    cy.get(`${dataCy('valium-banner-hydrator')} button`).click();
  });

  it('and providing right runtime arguments should pass the pipeline preview.', () => {
    if (skipPreviewTests) {
      skip();
    }
    // Since the preview already ran with invalid macros (stored in the state)
    // we need to configure the preview before running.
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
    cy.get(dataCy('preview-configure-run-btn')).click();
    cy.get(`${dataCy('valium-banner-hydrator')}`).contains(PREVIEW_SUCCESS_BANNER_MSG, {
      timeout: PIPELINE_RUN_TIMEOUT,
    });
    cy.get(`${dataCy('valium-banner-hydrator')} button`).click();
  });
});

describe('Deploying pipeline with temporary runtime arguments', () => {
  const runtimeArgsPipeline = `runtime_args_pipeline_${Date.now()}`;
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
    ).then((subject) => {
      expect(subject.length).to.be.eq(1);
    });
    cy.get('[title="Airport_source"').should('exist');
    cy.get('.pipeline-name').click();
    cy.get('#pipeline-name-input')
      .clear()
      .type(runtimeArgsPipeline)
      .type('{enter}');
    cy.get(dataCy('deploy-pipeline-btn')).click();
    cy.get(dataCy('Deployed')).should('exist');
  });

  it('and running it should succeed.', () => {
    cy.get('.arrow-btn-container').click();
    cy.get(dataCy(RUNTIME_ARGS_MODELESS_LOADING_SELECTOR)).should('not.exist');
    cy.get(dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)).should('exist');
    cy.update_runtime_args_row(0, 'source_path', 'random value 1', true);
    cy.update_runtime_args_row(1, 'sink_path', 'random value 2', true);
    cy.assert_runtime_args_row(0, 'source_path', 'random value 1');
    cy.assert_runtime_args_row(1, 'sink_path', 'random value 2');
    cy.add_runtime_args_row_with_value(2, 'test key 3', 'test value 3');
    cy.assert_runtime_args_row(2, 'test key 3', 'test value 3', false);
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(2)} ${dataCy('remove-row')}`
    ).click();
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(2)} ${dataCy(RUNTIME_ARGS_KEY_SELECTOR)}`
    ).should('not.exist');
    cy.get(dataCy('run-deployed-pipeline-modal-btn')).click();
    cy.get(dataCy('Failed'), { timeout: PIPELINE_RUN_TIMEOUT }).should('exist');
    cy.get('.arrow-btn-container').click();
    cy.update_runtime_args_row(0, 'source_path', SOURCE_PATH_VAL, true);
    cy.update_runtime_args_row(1, 'sink_path', SINK_PATH_VAL, true);
    cy.get(dataCy('run-deployed-pipeline-modal-btn')).click();
    cy.get(dataCy('Succeeded'), { timeout: PIPELINE_RUN_TIMEOUT }).should('exist');
  });

  it('should not persist unsaved arguments', () => {
    cy.get('.arrow-btn-container').click();
    cy.get(dataCy(RUNTIME_ARGS_MODELESS_LOADING_SELECTOR)).should('not.exist');
    cy.get(dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)).should('exist');
    cy.add_runtime_args_row_with_value(2, 'runtime_args_key2', 'runtime_args_value2');
    cy.add_runtime_args_row_with_value(3, 'runtime_args_key3', 'runtime_args_value3');
    // dismissing the modeless by clicking outside.
    cy.get('h1.pipeline-name').click();
    cy.get('.arrow-btn-container').click();
    cy.get(dataCy(RUNTIME_ARGS_MODELESS_LOADING_SELECTOR)).should('not.exist');
    cy.get(dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)).should('exist');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(2)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )}`
    ).should('not.exist');
  });
});

describe('Deploying pipeline with saved runtime arguments', () => {
  const runtimeArgsPipeline = `runtime_args_pipeline_${Date.now()}`;
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
    ).then((subject) => {
      expect(subject.length).to.be.eq(1);
    });
    cy.get('[title="Airport_source"').should('exist');
    cy.get('.pipeline-name').click();
    cy.get('#pipeline-name-input')
      .clear()
      .type(runtimeArgsPipeline)
      .type('{enter}');
    cy.get(dataCy('deploy-pipeline-btn')).click();
    cy.get(dataCy('Deployed')).should('exist');
  });

  it('and running it should succeed.', () => {
    cy.get('.arrow-btn-container').click();
    cy.get(dataCy(RUNTIME_ARGS_MODELESS_LOADING_SELECTOR)).should('not.exist');
    cy.get(dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)).should('exist');
    cy.assert_runtime_args_row(0, 'source_path', '', true);
    cy.assert_runtime_args_row(1, 'sink_path', '', true);
    cy.update_runtime_args_row(0, 'source_path', SOURCE_PATH_VAL, true);
    cy.update_runtime_args_row(1, 'sink_path', SINK_PATH_VAL, true);
    cy.get(dataCy('save-runtime-args-btn')).click();
    cy.get(dataCy('save-runtime-args-btn')).should('not.be.visible');
    cy.get(dataCy('pipeline-run-btn')).should('be.visible');
    cy.get(dataCy('pipeline-run-btn')).click({ force: true });
    cy.get(dataCy('Succeeded'), { timeout: PIPELINE_RUN_TIMEOUT }).should('exist');
  });

  it('should have saved runtime arguments available and validating other valid actions with runtime arguments', () => {
    // Verifying values are persisted
    cy.get('.arrow-btn-container').click();
    cy.assert_runtime_args_row(0, 'source_path', SOURCE_PATH_VAL, true);
    cy.assert_runtime_args_row(1, 'sink_path', SINK_PATH_VAL, true);
    // adding bunch of rows
    cy.add_runtime_args_row_with_value(2, 'runtime_args_key2', 'runtime_args_value2');
    cy.add_runtime_args_row_with_value(3, 'runtime_args_key3', 'runtime_args_value3');
    cy.add_runtime_args_row_with_value(4, 'runtime_args_key4', 'runtime_args_value4');
    cy.assert_runtime_args_row(2, 'runtime_args_key2', 'runtime_args_value2');
    cy.assert_runtime_args_row(3, 'runtime_args_key3', 'runtime_args_value3');
    cy.assert_runtime_args_row(4, 'runtime_args_key4', 'runtime_args_value4');
    // saving previously entered arguments and openeing the modal again to verify
    cy.get(dataCy('save-runtime-args-btn')).click();
    cy.get(dataCy('save-runtime-args-btn')).should('not.be.visible');
    cy.get('.arrow-btn-container').click();
    cy.assert_runtime_args_row(2, 'runtime_args_key2', 'runtime_args_value2');
    cy.assert_runtime_args_row(3, 'runtime_args_key3', 'runtime_args_value3');
    cy.assert_runtime_args_row(4, 'runtime_args_key4', 'runtime_args_value4');
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(2)} ${dataCy('remove-row')}`
    ).click();
    // row 3 and 4 becomes row 2 and 3
    cy.assert_runtime_args_row(2, 'runtime_args_key3', 'runtime_args_value3', false);
    cy.assert_runtime_args_row(3, 'runtime_args_key4', 'runtime_args_value4', false);
    // removing middle row ( row 2 ), row 3 becomes row 2, deleting that too
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(2)} ${dataCy('remove-row')}`
    ).click();
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(2)} ${dataCy('remove-row')}`
    ).click();
    // we should be left with original macros
    cy.get(
      `${dataCy(RUNTIME_ARGS_DEPLOYED_SELECTOR)} ${dataCy(2)} ${dataCy(
        RUNTIME_ARGS_VALUE_SELECTOR
      )}`
    ).should('not.exist');
    cy.assert_runtime_args_row(0, 'source_path', SOURCE_PATH_VAL, true);
    cy.assert_runtime_args_row(1, 'sink_path', SINK_PATH_VAL, true);
  });
});
