/*
 * Copyright © 2015 Cask Data, Inc.
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

angular.module(PKG.name + '.services')
  .constant('GLOBALS', {
    // Should be under property called 'artifactTypes' to be consistent. GLOBALS.etlBatch doesn't make much sense.
    etlBatch: 'cdap-etl-batch',
    etlRealtime: 'cdap-etl-realtime',
    // This is here because until now we don't store the aritfact information for each plugin anywhere.
    // From now we need this information to ask for backend widgets json. So if there are any published pipelines/drafts that
    // does not have artifact info of a plugin we default it to here.

    artifact: {
      default: {
        name: 'cdap-etl-lib',
        version: '3.3.0-SNAPSHOT',
        scope: 'SYSTEM'
      }
    },
    pluginTypes: {
      'cdap-etl-batch': {
        'source': 'batchsource',
        'sink': 'batchsink',
        'transform': 'transform'
      },
      'cdap-etl-realtime': {
        'source': 'realtimesource',
        'sink': 'realtimesink',
        'transform': 'transform'
      }
    },
    pluginConvert: {
      'batchsource': 'source',
      'realtimesource': 'source',
      'batchsink': 'sink',
      'realtimesink': 'sink',
      'transform': 'transform'
    },

    'en': {
      hydrator: {
        appLabel: 'Hydrator Pipeline',
        studio: {
          defaultReferenceInfo: 'Please select a plugin to view reference information',
          noReferenceInfo: 'Currently, no reference information is available for this plugin.',
          noConfigMessage: 'No configuration found for the plugin.',
          syntaxConfigJsonError: 'Syntax error in the configuration JSON for the plugin.',
          semanticConfigJsonError: 'Semantic error in the configuration JSON for the plugin.',
          oneSinkError: 'Please add at least one sink to your pipeline.',
          sourceRequiredFieldsError: 'Please provide required fields for the source.',
          sinkRequiredFieldsError: 'Please provide required fields for the sink.',
          transformRequiredFieldsError: 'Please provide required fields for the transform.',
          genericMissingRequiredFieldsError: 'Missing required fields.',
          pluginNameMissingRequiredFieldsError: ' is missing required fields',
          oneSourceError: 'Pipelines can only have one source. Please remove any additional sources.',
          noSourceError: 'Please add a source to your pipeline',
          nameError: 'Please name your pipeline.',
          nameValidationError: 'Pipeline names can only contain alphanumeric (\'a-z A-Z 0-9\') and underscore ( \'_\') characters. Please remove any other characters.',
          sinkBranchNodeError: 'Please connect multiple sinks to the same node.',
          atLeastOneSinkError: 'Please add a sink to your pipeline',
          branchError: 'Please remove branched connections.',
          unconnectedNodesError: 'Please connect all nodes.',
          circularConnectionError: 'Please remove the circular connection in this pipeline.',
          endSinkError: 'Please end the pipeline connections in a sink.',
          parallelConnectionError: 'Please remove parallel connections in this pipeline.',
          pluginDoesNotExist: 'This plugin does not exist: ',
          unsavedPluginMessage1: 'There are unsaved changes for node \'',
          unsavedPluginMessage2: '\'. Please save them before publishing the pipeline.',
          pipelineNameAlreadyExistError: 'A pipeline with this name already exists. Please choose a different name.'
        },
        wizard: {
          welcomeMessage1: 'Hydrator makes it easy to prepare data so you ',
          welcomeMessage2: 'can get down to business faster. Let\'s get started!',
          createMessage: 'ETL made simple. Hydrator offers four ways to get started.',
          createConsoleMessage: 'Click a node from the menu to place it on the canvas above.'
        },
      },
      admin: {
        templateNameExistsError: 'This template name already exists! Please choose another name.',
        pluginSameNameError: 'There is already a plugin with this name.',
        templateNameMissingError: 'Please enter a template name.'
      }
    }
  });
