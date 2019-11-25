/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

// One place to edit the group label.
const pluginLabels = {
  'source': 'Source',
  'transform': 'Transform',
  'analytics': 'Analytics',
  'sink': 'Sink',
  'action': 'Conditions and Actions',
  'erroralert': 'Error Handlers and Alerts'
};
const NUMBER_TYPES = ['integer', 'int', 'short', 'long', 'float', 'double'];
const GLOBALS = {
  etlBatch: 'cdap-etl-batch',
  etlRealtime: 'cdap-etl-realtime',
  etlDataStreams: 'cdap-data-streams',
  etlDataPipeline: 'cdap-data-pipeline',
  etlBatchPipelines: ['cdap-etl-batch', 'cdap-data-pipeline'],
  etlPipelineTypes: ['cdap-data-streams', 'cdap-data-pipeline'],
  // Map defines what plugin types to surface for each artifact in UI.
  pluginTypes: {
    'cdap-etl-batch': {
      'source': 'batchsource',
      'sink': 'batchsink',
      'transform': 'transform',
    },
    'cdap-data-streams':{
      'transform': 'transform',
      'source': 'streamingsource',
      'sparkcompute': 'sparkcompute',
      'batchaggregator': 'batchaggregator',
      'sparksink': 'sparksink',
      'sink': 'batchsink',
      'batchjoiner': 'batchjoiner',
      'sparkjoiner': 'sparkjoiner',
      'windower': 'windower',
      'errortransform': 'errortransform',
      'alertpublisher': 'alertpublisher',
      'condition': 'condition',
      'splittertransform': 'splittertransform'

    },
    'cdap-etl-realtime': {
      'source': 'realtimesource',
      'sink': 'realtimesink',
      'transform': 'transform'
    },
    'cdap-data-pipeline': {
      'source': 'batchsource',
      'sink': 'batchsink',
      'transform': 'transform',
      'batchaggregator': 'batchaggregator',
      'sparksink': 'sparksink',
      'sparkcompute': 'sparkcompute',
      'batchjoiner': 'batchjoiner',
      'sparkjoiner': 'sparkjoiner',
      'action': 'action',
      'errortransform': 'errortransform',
      'sparkprogram': 'sparkprogram',
      'alertpublisher': 'alertpublisher',
      'condition': 'condition',
      'splittertransform': 'splittertransform'
    },
    'post-run-actions': {
      'email': 'Send Email',
      'databasequery': 'Run Database Query',
      'httpcallback': 'Make HTTP Call'
    }
  },
  'pluginTypeToLabel': {
    'transform': pluginLabels['transform'],
    'batchsource': pluginLabels['source'],
    'batchsink': pluginLabels['sink'],
    'batchaggregator': pluginLabels['analytics'],
    'realtimesink': pluginLabels['sink'],
    'realtimesource': pluginLabels['source'],
    'sparksink': pluginLabels['sink'],
    'sparkcompute': pluginLabels['analytics'],
    'batchjoiner': pluginLabels['analytics'],
    'sparkjoiner': pluginLabels['analytics'],
    'action': pluginLabels['action'],
    'streamingsource': pluginLabels['source'],
    'windower': pluginLabels['transform'],
    'errortransform': pluginLabels['erroralert'],
    'sparkprogram': pluginLabels['action'],
    'alertpublisher': pluginLabels['erroralert'],
    'condition': pluginLabels['action'],
    'splittertransform': pluginLabels['transform']
  },
  pluginLabels: pluginLabels,
  // understand what plugin type is what.
  // if we get batchaggregator from backend it is marked as transform here.
  pluginConvert: {
    'batchaggregator': 'transform',
    'streamingsource': 'source',
    'windower': 'transform',
    'batchsource': 'source',
    'realtimesource': 'source',
    'batchsink': 'sink',
    'realtimesink': 'sink',
    'transform': 'transform',
    'sparksink': 'sink',
    'sparkcompute': 'transform',
    'batchjoiner': 'transform',
    'sparkjoiner': 'transform',
    'action': 'action',
    'errortransform': 'transform',
    'sparkprogram': 'action',
    'alertpublisher': 'sink',
    'condition': 'condition',
    'splittertransform': 'transform'
  },

  artifactConvert: {
    'cdap-etl-batch': 'Batch (Deprecated)',
    'cdap-etl-realtime': 'Realtime (Deprecated)',
    'cdap-data-pipeline': 'Data Pipeline - Batch',
    'cdap-data-streams': 'Data Pipeline - Realtime'
  },

  iconArtifact: {
    'cdap-etl-batch': 'ETLBatch',
    'cdap-etl-realtime': 'ETLRealtime',
    'cdap-data-pipeline': 'ETLBatch',
    'cdap-data-streams': 'sparkstreaming'
  },

  defaultSchemaName: 'etlSchemaBody',
  defaultScheduleId: 'dataPipelineSchedule',

  programType: {
    'cdap-data-pipeline': 'workflows',
    'cdap-data-streams': 'spark'
  },

  programId: {
    'cdap-data-pipeline': 'DataPipelineWorkflow',
    'cdap-data-streams': 'DataStreamsSparkStreaming'
  },

  wrangler: {
    pluginArtifactName: 'wrangler-transform'
  },

  'en': {
    hydrator: {
      appLabel: 'Hydrator Pipeline',
      studio: {
        info: {
          'DEFAULT-REFERENCE': 'Please select a plugin to view reference information',
          'NO-REFERENCE': 'Currently, no reference information is available for this plugin.',
          'NO-CONFIG': 'No widgets JSON found for the plugin. Please check documentation on how to add.',
          'ARTIFACT-UPLOAD-MESSAGE-JAR': 'The plugin JAR needs to be a JAR file.',
          'ARTIFACT-UPLOAD-MESSAGE-JSON': 'The plugin JSON needs to be a JSON file.',
          'ARTIFACT-UPLOAD-ERROR-JSON': 'Error in parsing config json for the artifact.'
        },
        error: {
          'SYNTAX-CONFIG-JSON': 'Error parsing widgets JSON for the plugin. Please check the documentation to fix.',
          'SEMANTIC-CONFIG-JSON': 'Semantic error in the configuration JSON for the plugin.',
          'GENERIC-MISSING-REQUIRED-FIELDS': 'Please provide required information.',
          'MISSING-REQUIRED-FIELDS': 'is missing required fields',
          'INVALID-VALUE-FIELDS': 'invalid value in fields',
          'NO-SOURCE-FOUND': 'Please add a source to your pipeline',
          'MISSING-NAME': 'Pipeline name is missing.',
          'INVALID-NAME': 'Pipeline names can only contain alphanumeric (\'a-z A-Z 0-9\'), underscore (\'_\'), and hyphen (\'-\') characters. Please remove any other characters.',
          'MISSING-RESOURCES': 'Pipeline resources missing value (Memory MB)',
          'MISSING-DRIVERRESOURCES': 'Pipeline driver resources missing value (Memory MB)',
          'MISSING-CLIENTRESOURCES': 'Pipeline client resources missing value (Memory MB)',
          'INVALID-RESOURCES': 'Pipeline resources (Memory MB) should be positive numbers',
          'INVALID-DRIVERRESOURCES': 'Pipeline driver resources (Memory MB) should be positive numbers',
          'INVALID-CLIENTRESOURCES': 'Pipeline client resources (Memory MB) should be positive numbers',
          'NO-SINK-FOUND': 'Please add a sink to your pipeline',
          'NAME-ALREADY-EXISTS': 'A pipeline with this name already exists. Please choose a different name.',
          'DUPLICATE-NODE-NAMES': 'Every node should have a unique name to be exported/published.',
          'DUPLICATE-NAME': 'Node with the same name already exists.',
          'MISSING-CONNECTION': 'is missing connection',
          'IMPORT-JSON': {
            'INVALID-ARTIFACT': 'Pipeline configuration should have a valild artifact specification.',
            'INVALID-CONFIG': 'Missing \'config\' property in pipeline specification.',
            'INVALID-SOURCE': 'Pipeline configuration should have a valid source specification.',
            'INVALID-SINKS': 'Pipeline configuration should have a valid sink specification.',
            'INVALID-SCHEDULE': 'Batch pipeline should have a valid schedule specification.',
            'INVALID-INSTANCE': 'Realtime pipeline should have a valid instance specification.',
            'INVALID-NODES-CONNECTIONS': 'Unknown node(s) in \'connections\' property in pipeline specification.',
            'NO-STAGES':  'Missing \'stages\' property in config specification.',
            'INVALID-STAGES':  'Found \'stages\' property outside of config specification.',
            'INVALID-CONNECTIONS': 'Found \'connections\' property outside of config specification.',
            'INVALID-SYNTAX': 'Syntax Error. Ill-formed pipeline configuration'
          },
          'PREVIEW': {
            'NO-SOURCE-SINK': 'Please add a source and sink to the pipeline'
          },
          'MISSING-SYSTEM-ARTIFACTS': 'The artifacts required for this data pipeline are still being loaded. Please try again in a few minutes'
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
};
const HYDRATOR_DEFAULT_VALUES = {
  instance: 1,
  batchInterval: '10s',
  schedule: '0 * * * *',
  resources: {
    virtualCores: 1,
    memoryMB: 1024
  },
  numOfRecordsPreview: 100,
  previewTimeoutInMin: 2,
  engine: 'mapreduce',
  processTimingEnabled: true,
  stageLoggingEnabled: true,
  disableCheckpoints: false,
  stopGracefully: true,
  backpressure: true,
  numExecutors: 1
};

const PROGRAM_STATUSES = {
  DEPLOYED: 'DEPLOYED',
  SUBMITTING: 'SUBMITTING',
  RUNNING: 'RUNNING',
  SUCCEEDED: 'SUCCEEDED',
  FAILED: 'FAILED',
  DRAFT: 'DRAFT',
  STOPPED: 'STOPPED',
  COMPLETED: 'COMPLETED',
  KILLED: 'KILLED',
  KILLED_BY_TIMER: 'KILLED_BY_TIMER',
  DEPLOY_FAILED: 'DEPLOY_FAILED',
  RUN_FAILED: 'RUN_FAILED',
  SUSPENDED: 'SUSPENDED',
  SCHEDULED: 'SCHEDULED',
  STARTING: 'STARTING',
  SCHEDULING: 'SCHEDULING',
  STOPPING: 'STOPPING',
  SUSPENDING: 'SUSPENDING',
  PENDING: 'PENDING'
};

const SECURE_KEY_PREFIX = '${secure(';
const SECURE_KEY_SUFFIX = ')}';

const CLOUD = {
  DEFAULT_PROFILE_NAME: 'SYSTEM:native',
  PROFILE_NAME_PREFERENCE_PROPERTY: 'system.profile.name',
  PROFILE_PROPERTIES_PREFERENCE: 'system.profile.properties'
};

const SCOPES = {
  SYSTEM: 'SYSTEM',
  USER: 'USER'
};

const SYSTEM_NAMESPACE = 'system';

export {
  NUMBER_TYPES,
  GLOBALS,
  HYDRATOR_DEFAULT_VALUES,
  PROGRAM_STATUSES,
  SECURE_KEY_PREFIX,
  SECURE_KEY_SUFFIX,
  CLOUD,
  SCOPES,
  SYSTEM_NAMESPACE
};
