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

const ExploreDatasetActions = {
  onReset: 'EDA-RESET-STORE',
  updateActionType: 'EDA-UPDATE-ACTION-TYPE',
  updatePipelineName: 'EDA-UPDATE-PIPELINE-NAME',
  setSchema:  'EDA-SET-SCHEMA',
  setAvailableOperations: 'EDA-SET-AVAILABLE-OPERATIONS',
  setAvailableEngineConfigurations: 'EDA-SET-AVAILABLE-ENGINE-CONFIGURATIONS',
  setAvailableSinks: 'EDA-SET-AVAILABLE-SINKS',
  updateEngineConfigurations: 'EDA-UPDATE-ENGINE-CONFIGURATIONS',
  updateOperationConfigurations: 'EDA-UPDATE-OPERATION-CONFIGURATION',
  setExtraConfigurations: 'EDA-SET-EXTRA-CONFIGURATIONS',
  setSinkConfigurations: 'EDA-SET-SINK-CONFIGURATIONS',
};

export default ExploreDatasetActions;
