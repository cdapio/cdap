/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
var PipelineNodeMetricsGraph = require('../cdap/components/PipelineNodeGraphs/PipelineNodeMetricsGraph')
  .default;
var CDAPHelpers = require('../cdap/services/helpers');
var RulesEngineHome = require('../cdap/components/RulesEngineHome').default;
var Mousetrap = require('mousetrap');
var StatusFactory = require('../cdap/services/StatusFactory').default;
var LoadingIndicator = require('../cdap/components/LoadingIndicator').default;
var StatusAlertMessage = require('../cdap/components/StatusAlertMessage').default;
var PipelineTriggersSidebars = require('../cdap/components/PipelineTriggersSidebars').default;
var TriggeredPipelineStore = require('../cdap/components/TriggeredPipelines/store/TriggeredPipelineStore')
  .default;
var PipelineErrorFactory = require('../cdap/services/PipelineErrorFactory');
var GLOBALS = require('../cdap/services/global-constants').GLOBALS;
var PROGRAM_STATUSES = require('../cdap/services/global-constants').PROGRAM_STATUSES;
var HYDRATOR_DEFAULT_VALUES = require('../cdap/services/global-constants').HYDRATOR_DEFAULT_VALUES;
var StatusMapper = require('../cdap/services/StatusMapper').default;
var PipelineDetailStore = require('../cdap/components/PipelineDetails/store').default;
var PipelineDetailActionCreator = require('../cdap/components/PipelineDetails/store/ActionCreator');
var PipelineDetailsTopPanel = require('../cdap/components/PipelineDetails/PipelineDetailsTopPanel')
  .default;
var PipelineScheduler = require('../cdap/components/PipelineScheduler').default;
var AvailablePluginsStore = require('../cdap/services/AvailablePluginsStore').default;
var AVAILABLE_PLUGINS_ACTIONS = require('../cdap/services/AvailablePluginsStore')
  .AVAILABLE_PLUGINS_ACTIONS;
var PipelineDetailsRunLevelInfo = require('../cdap/components/PipelineDetails/RunLevelInfo')
  .default;
var MetricsQueryHelper = require('../cdap/services/MetricsQueryHelper').default;
var PipelineMetricsStore = require('../cdap/services/PipelineMetricsStore').default;
var PipelineMetricsActionCreator = require('../cdap/services/PipelineMetricsStore/ActionCreator');
var PipelineConfigurationsActionCreator = require('../cdap/components/PipelineConfigurations/Store/ActionCreator');
var ThemeHelper = require('../cdap/services/ThemeHelper');
var Footer = require('../cdap/components/Footer').default;
var cdapavscwrapper = require('../cdap/services/cdapavscwrapper').default;
var IconSVG = require('../cdap/components/IconSVG').default;
var PipelineConfigConstants = require('../cdap/components/PipelineConfigurations/PipelineConfigConstants');
var AuthRefresher = require('../cdap/components/AuthRefresher').default;
var ToggleSwitch = require('../cdap/components/ToggleSwitch').default;
var PipelineList = require('../cdap/components/PipelineList').default;
var AppHeader = require('../cdap/components/AppHeader').default;
var Markdown = require('../cdap/components/Markdown').MarkdownWithStyles;
var CodeEditor = require('../cdap/components/AbstractWidget/CodeEditorWidget').default;
var JSONEditor = require('../cdap/components/CodeEditor/JSONEditor').default;
var TextBox = require('../cdap/components/AbstractWidget/FormInputs/TextBox').default;
var Number = require('../cdap/components/AbstractWidget/FormInputs/Number').default;
var CSVWidget = require('../cdap/components/AbstractWidget/CSVWidget').default;
var KeyValueWidget = require('../cdap/components/AbstractWidget/KeyValueWidget').default;
var Select = require('../cdap/components/AbstractWidget/FormInputs/Select').default;
var KeyValueDropdownWidget = require('../cdap/components/AbstractWidget/KeyValueDropdownWidget')
  .default;
var MultipleValuesWidget = require('../cdap/components/AbstractWidget/MultipleValuesWidget')
  .default;
var PluginConnectionBrowser = require('../cdap/components/DataPrepConnections/PluginConnectionBrowser')
  .default;
var FunctionDropdownAlias = require('../cdap/components/AbstractWidget/FunctionDropdownAliasWidget')
  .default;
var ToggleSwitchWidget = require('../cdap/components/AbstractWidget/ToggleSwitchWidget').default;
var WranglerEditor = require('../cdap/components/AbstractWidget/WranglerEditor').default;
var RadioGroupWidget = require('../cdap/components/AbstractWidget/RadioGroupWidget').default;
var MultiSelect = require('../cdap/components/AbstractWidget/FormInputs/MultiSelect').default;
var JoinTypeWidget = require('../cdap/components/AbstractWidget/JoinTypeWidget').default;
var InputFieldDropdown = require('../cdap/components/AbstractWidget/InputFieldDropdown').default;
var DatasetSelectorWidget = require('../cdap/components/AbstractWidget/DatasetSelectorWidget')
  .default;
var SqlConditionsWidget = require('../cdap/components/AbstractWidget/SqlConditionsWidget').default;
var SqlSelectorWidget = require('../cdap/components/AbstractWidget/SqlSelectorWidget').default;
var KeyValueEncodedWidget = require('../cdap/components/AbstractWidget/KeyValueWidget/KeyValueEncodedWidget')
  .default;
var SessionTokenStore = require('../cdap/services/SessionTokenStore');
var ConfigurationGroup = require('../cdap/components/ConfigurationGroup').default;
var WidgetWrapper = require('../cdap/components/ConfigurationGroup/WidgetWrapper')
  .WrappedWidgetWrapper;
var ConfigurationGroupUtilities = require('../cdap/components/ConfigurationGroup/utilities');

export {
  Store,
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
  PROGRAM_STATUSES,
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
  PipelineConfigurationsActionCreator,
  ThemeHelper,
  Footer,
  cdapavscwrapper,
  IconSVG,
  PipelineConfigConstants,
  AuthRefresher,
  ToggleSwitch,
  PipelineList,
  AppHeader,
  Markdown,
  CodeEditor,
  JSONEditor,
  TextBox,
  Number,
  CSVWidget,
  KeyValueWidget,
  Select,
  KeyValueDropdownWidget,
  MultipleValuesWidget,
  PluginConnectionBrowser,
  FunctionDropdownAlias,
  ToggleSwitchWidget,
  WranglerEditor,
  RadioGroupWidget,
  MultiSelect,
  JoinTypeWidget,
  InputFieldDropdown,
  DatasetSelectorWidget,
  SqlConditionsWidget,
  SqlSelectorWidget,
  KeyValueEncodedWidget,
  SessionTokenStore,
  ConfigurationGroup,
  WidgetWrapper,
  ConfigurationGroupUtilities,
};
