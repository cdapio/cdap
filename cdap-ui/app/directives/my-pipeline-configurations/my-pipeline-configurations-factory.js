/*
 * Copyright © 2017 Cask Data, Inc.
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

angular.module(PKG.name + '.commons')
  .factory('MyPipelineConfigFactory', function() {
    let attributes = {
      'runtime-arguments': 'runtimeArguments',
      'resolved-macros': 'resolvedMacros',
      'apply-runtime-arguments': 'applyRuntimeArguments()',
      'convert-runtime-args-to-macros': 'convertRuntimeArgsToMacros()',
      'pipeline-name': '{{::pipelineName}}',
      'pipeline-action': '{{::pipelineAction}}',
      'run-pipeline': 'runPipeline()',
      'on-close': 'onClose()',
      'namespace': 'namespace',
      'store': 'store',
      'action-creator': 'actionCreator',
      'is-disabled': 'isDisabled',
      'show-preview-config': 'showPreviewConfig'
    };
    let batchPipelineConfig = {
      'element': '<my-batch-pipeline-config></my-batch-pipeline-config>',
      'attributes': attributes
    };
    let realtimePipelineConfig = {
      'element': '<my-realtime-pipeline-config></my-realtime-pipeline-config>',
      'attributes': attributes
    };
    return {
      'cdap-etl-batch': batchPipelineConfig,
      'cdap-data-pipeline': batchPipelineConfig,
      'cdap-data-streams': realtimePipelineConfig,
      'cdap-etl-realtime': realtimePipelineConfig
    };
  });
