/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

import T from 'i18n-react';
T.setTexts(require('../cdap/text/text-en.yaml'));
var Store = require('../cdap/services/NamespaceStore').default;
var Header = require('../cdap/components/Header').default;
var ResourceCenterButton = require('../cdap/components/ResourceCenterButton').default;
var DataPrepHome = require('../cdap/components/DataPrepHome').default;
var DataPrepHelper = require('../cdap/components/DataPrep/helper');
var globalEvents = require('../cdap/services/global-events').default;
var ee = require('event-emitter');
var VersionStore = require('../cdap/services/VersionStore').default;
var VersionActions = require('../cdap/services/VersionStore/VersionActions').default;
var Version = require('../cdap/services/VersionRange/Version').default;
var VersionRange = require('../cdap/services/VersionRange').default;
var KeyValuePairs = require('../cdap/components/KeyValuePairs').default;
var KeyValueStore = require('../cdap/components/KeyValuePairs/KeyValueStore').default;
var KeyValueStoreActions = require('../cdap/components/KeyValuePairs/KeyValueStoreActions').default;
var PipelineSummary = require('../cdap/components/PipelineSummary').default;
var PipelineNodeMetricsGraph = require('../cdap/components/PipelineNodeGraphs/PipelineNodeMetricsGraph').default;
var CDAPHelpers = require('../cdap/services/helpers');
var RulesEngineHome = require('../cdap/components/RulesEngineHome').default;
var Mousetrap = require('mousetrap');
var StatusFactory = require('../cdap/services/StatusFactory').default;
var LoadingIndicator = require('../cdap/components/LoadingIndicator').default;
var StatusAlertMessage = require('../cdap/components/StatusAlertMessage').default;
var PipelineTriggersSidebars = require('../cdap/components/PipelineTriggersSidebars').default;
var TriggeredPipelineStore = require('../cdap/components/TriggeredPipelines/store/TriggeredPipelineStore').default;
var PipelineErrorFactory = require('../cdap/services/PipelineErrorFactory');
var GLOBALS = require('../cdap/services/global-constants').GLOBALS;
var HYDRATOR_DEFAULT_VALUES = require('../cdap/services/global-constants').HYDRATOR_DEFAULT_VALUES;
var StatusMapper = require('../cdap/services/StatusMapper').default;
var PipelineDetailStore = require('../cdap/components/PipelineDetails/store').default;
var PipelineDetailActionCreator = require('../cdap/components/PipelineDetails/store/ActionCreator');
var PipelineDetailsTopPanel = require('../cdap/components/PipelineDetails/PipelineDetailsTopPanel').default;
var PipelineScheduler = require('../cdap/components/PipelineScheduler').default;
var AvailablePluginsStore = require('../cdap/services/AvailablePluginsStore').default;
var AVAILABLE_PLUGINS_ACTIONS = require('../cdap/services/AvailablePluginsStore').AVAILABLE_PLUGINS_ACTIONS;
var PipelineDetailsRunLevelInfo = require('../cdap/components/PipelineDetails/RunLevelInfo').default;
var MetricsQueryHelper = require('../cdap/services/MetricsQueryHelper').default;
var PipelineMetricsStore = require('../cdap/services/PipelineMetricsStore').default;
var PipelineMetricsActionCreator = require('../cdap/services/PipelineMetricsStore/ActionCreator');
var PipelineConfigurationsActionCreator = require('../cdap/components/PipelineConfigurations/Store/ActionCreator');

export {
  Store,
  Header,
  DataPrepHome,
  DataPrepHelper,
  globalEvents,
  ee,
  VersionStore,
  VersionActions,
  VersionRange,
  Version,
  ResourceCenterButton,
  KeyValuePairs,
  KeyValueStore,
  KeyValueStoreActions,
  Mousetrap,
  PipelineSummary,
  PipelineNodeMetricsGraph,
  CDAPHelpers,
  StatusFactory,
  LoadingIndicator,
  StatusAlertMessage,
  RulesEngineHome,
  PipelineTriggersSidebars,
  TriggeredPipelineStore,
  PipelineErrorFactory,
  GLOBALS,
  HYDRATOR_DEFAULT_VALUES,
  StatusMapper,
  PipelineDetailStore,
  PipelineDetailActionCreator,
  PipelineDetailsTopPanel,
  PipelineScheduler,
  AvailablePluginsStore,
  AVAILABLE_PLUGINS_ACTIONS,
  PipelineDetailsRunLevelInfo,
  MetricsQueryHelper,
  PipelineMetricsStore,
  PipelineMetricsActionCreator,
  PipelineConfigurationsActionCreator
};
