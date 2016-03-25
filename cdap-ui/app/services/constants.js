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
          info: {
            'DEFAULT-REFERENCE': 'Please select a plugin to view reference information',
            'NO-REFERENCE': 'Currently, no reference information is available for this plugin.',
            'NO-CONFIG': 'No widgets JSON found for the plugin. Please check documentation on how to add.',
          },
          error: {
            'SYNTAX-CONFIG-JSON': 'Error parsing widgets JSON for the plugin. Please check the documentation to fix.',
            'SEMANTIC-CONFIG-JSON': 'Semantic error in the configuration JSON for the plugin.',
            'GENERIC-MISSING-REQUIRED-FIELDS': 'Please provide required information.',
            'MISSING-REQUIRED-FIELDS': 'is missing required fields',
            'MORE-THAN-ONE-SOURCE-FOUND': 'Pipelines can only have one source. Please remove any additional sources.',
            'NO-SOURCE-FOUND': 'Please add a source to your pipeline',
            'MISSING-NAME': 'Pipeline name is missing.',
            'INVALID-NAME': 'Pipeline names can only contain alphanumeric (\'a-z A-Z 0-9\') and underscore ( \'_\') characters. Please remove any other characters.',
            'NO-SINK-FOUND': 'Please add a sink to your pipeline',
            'NAME-ALREADY-EXISTS': 'A pipeline with this name already exists. Please choose a different name.',
            'DUPLICATE-NODE-NAMES': 'Every node should have a unique name to be exported/published.',
            'DUPLICATE-NAME': 'Node with the same name already exists.',
            'MISSING-CONNECTION': ' is missing connection',
            'IMPORT-JSON': {
              'INVALID-ARTIFACT': 'Pipeline configuration should have a valild artifact specification.',
              'INVALID-CONFIG': 'Missing \'config\' property in pipeline specification.',
              'INVALID-SOURCE': 'Pipeline configuration should have a valid source specification.',
              'INVALID-SINKS': 'Pipeline configuration should have a valid sink specification.',
              'INVALID-SCHEDULE': 'Batch pipeline should have a valid schedule specification.',
              'INVALID-INSTANCE': 'Realtime pipeline should have a valid instance specification.',
              'INVALID-NODES-CONNECTIONS': 'Unknown node(s) in \'connections\' property in pipeline specification.'
            }
          },
          pluginDoesNotExist: 'This plugin does not exist: '
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
        templateNameMissingError: 'Please enter a template name.',
        pluginTypeMissingError: 'Please choose a plugin type.',
        templateTypeMissingError: 'Please choose a template type.',
        pluginMissingError: 'Please choose a plugin.',
        pluginVersionMissingError: 'Please choose artifact version for the plugin.'
      }
    }
  });
