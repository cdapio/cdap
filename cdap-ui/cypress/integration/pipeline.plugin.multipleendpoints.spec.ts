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
const nullSplitterPipeline = `null_splitter_pipeline_${Date.now()}`;
const unionConditionPipeline = `union_condition_splitter_${Date.now()}`;
const pipelines = [nullSplitterPipeline, unionConditionPipeline];

describe('Pipelines with plugins having more than one endpoints', () => {
  afterEach(() => {
    // Delete the pipeline to clean up
    const headers = Helpers.getAuthHeaders();
    pipelines.forEach((pipeline) => cy.cleanup_pipelines(headers, pipeline));
  });

  function deployAndTestPipeline(filename, pipelineName) {
    cy.visit('/');
    cy.get('#resource-center-btn').click();
    cy.get('#create-pipeline-link').click();
    cy.url().should('include', '/studio');
    cy.upload_pipeline(filename, '#pipeline-import-config-link > input[type="file"]').then(() => {
      // This is arbitrary. Right now we don't have a way to determine
      // if the upgrade check is done. Since this a standalone the assumption
      // is this won't take more than 10 seconds.
      cy.wait(6000);
      // Name pipeline then deploy pipeline
      cy.get('.pipeline-name').click();
      cy.get('#pipeline-name-input')
        .clear()
        .type(pipelineName)
        .type('{enter}');
      cy.get('[data-testid=deploy-pipeline]').click();
      cy.wait(6000);
      cy.url().should('include', `/view/${pipelineName}`);
    });
  }
  it('Should work with union splitter and condition plugins', () => {
    deployAndTestPipeline(
      'union_condition_splitter_pipeline_v1-cdap-data-pipeline.json',
      unionConditionPipeline
    );
  });
  it('Should work with null splitter plugin', () => {
    deployAndTestPipeline('null_splitter_pipeline-cdap-data-pipeline.json', nullSplitterPipeline);
  });
});
