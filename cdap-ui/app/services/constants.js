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

    'en': {
      hydrator: {
        appLabel: 'Hydrator Pipeline',
        studio: {
          oneSinkError: 'Please add at least one sink to your pipeline.',
          sourceRequiredFieldsError: 'Please provide required fields for Source.',
          sinkRequiredFieldsError: 'Please provide required fields for Sink.',
          transformRequiredFieldsError: 'Please provide required fields for Transform.',
          oneSourceError: 'Pipelines can only have one source. Please remove any additional sources.',
          noSourceError: 'Please add a source to your pipeline',
          nameError: 'Please name your pipeline.',
          nameValidationError: 'Pipeline names can only contain letters of the alphabet, numbers, and \'_\' Please remove other character types.',
          sinkBranchNodeError: 'Please connect multiple sinks to the same node.',
          branchError: 'Please remove branched connections.',
          unconnectedNodesError: 'Please connect all nodes.',
          circularConnectionError: 'Please remove the circular connection in this pipeline.',
          endSinkError: 'Please end the pipeline connections in a sink.',
          parallelConnectionError: 'Please remove parallel connections in this pipeline.'
        },
        wizard: {
          welcomeMessage1: 'Hydrator makes it easy to prepare data so you',
          welcomeMessage2: 'can get down to business faster. Let’s get started!',
          createMessage: 'ETL made simple. Hydrator offers four ways to get started.',
          createConsoleMessage: 'Click a node from the menu to place it on the canvas above.'
        }
      }
    }
  });
