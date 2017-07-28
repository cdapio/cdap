/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
 var Mousetrap = require('mousetrap');
 var StatusFactory = require('../cdap/services/StatusFactory').default;
 var LoadingIndicator = require('../cdap/components/LoadingIndicator').default;
 var StatusAlertMessage = require('../cdap/components/StatusAlertMessage').default;

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
  StatusAlertMessage
};
