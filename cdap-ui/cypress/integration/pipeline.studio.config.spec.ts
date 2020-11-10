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

import { dataCy, loginIfRequired, getArtifactsPoll } from '../helpers';
let headers = {};

describe('Pipeline Studio Config', () => {
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
        win.sessionStorage.setItem('pipelineConfigTesting', 'true');
      },
    });
  });

  beforeEach(() => {
    getArtifactsPoll(headers);
  });

  after(() => {
    cy.window().then((win) => {
      win.sessionStorage.removeItem('pipelineConfigTesting');
    });
  });

  it('should support changing the engine config', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.create_simple_pipeline();

    cy.get(dataCy('pipeline-configure-modeless-btn')).click();
    cy.contains('Engine config').click();
    cy.contains('MapReduce').click();
    cy.get('[data-testid="config-apply-close"]').click();

    cy.get_pipeline_json().then((pipelineConfig) => {
      expect(pipelineConfig.config.engine).eq('mapreduce');
    });
  });

  it('should support setting an engine custom config', () => {
    const KEY_VALUE = 'key0';
    const VALUE_VALUE = 'value0;'

    cy.visit('/pipelines/ns/default/studio');
    cy.create_simple_pipeline();

    cy.get(dataCy('pipeline-configure-modeless-btn')).click();
    cy.contains('Engine config').click();
    cy.contains('MapReduce').click();

    cy.get(dataCy('engine-config-tab-custom')).click();
    cy.get(dataCy('key-value-pair-0')).within(() => {
      cy.get('input[placeholder="key"]')
        .clear()
        .type(KEY_VALUE);
      cy.get('input[placeholder="value"]')
        .clear()
        .type(VALUE_VALUE);
    });
    
    cy.get('[data-testid="config-apply-close"]').click();

    cy.get_pipeline_json().then((pipelineConfig) => {
      expect(pipelineConfig.config.engine).eq('mapreduce');
      expect(pipelineConfig.config.properties[`system.mapreduce.${KEY_VALUE}`]).eq(VALUE_VALUE);
    });
  });

  it('should support setting an engine custom config', () => {
    cy.visit('/pipelines/ns/default/studio');
    cy.create_simple_pipeline();

    cy.get(dataCy('pipeline-configure-modeless-btn')).click();
    cy.contains('Engine config').click();
    cy.contains('MapReduce').click();

    cy.contains('Resources').click();
    cy.get(dataCy('resources-config-tab-driver')).within(() => {
      cy.get('[ng-model="virtualCores"]').select('2');
      cy.get('[ng-model="internalModel"]').clear().type('3072');
    });
    cy.get(dataCy('resources-config-tab-executor')).within(() => {
      cy.get('[ng-model="virtualCores"]').select('3');
      cy.get('[ng-model="internalModel"]').clear().type('4096');
    });

    cy.get('[data-testid="config-apply-close"]').click();

    cy.get_pipeline_json().then((pipelineConfig) => {
      const { driverResources, resources } = pipelineConfig.config;
      expect(driverResources.memoryMB).eq(3072);
      expect(driverResources.virtualCores).eq(2);

      expect(resources.memoryMB).eq(4096);
      expect(resources.virtualCores).eq(3);
    });
  });
});