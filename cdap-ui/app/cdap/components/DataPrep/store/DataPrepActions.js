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

const DataPrepActions = {
  setData: 'DATAPREP_SET_DATA',
  setDirectives: 'DATAPREP_SET_DIRECTIVES',
  setProperties: 'DATAPREP_SET_PROPERTIES',
  setWorkspace: 'DATAPREP_SET_WORKSPACE',
  setWorkspaceId: 'DATAPREP_SET_WORKSPACE_ID',
  setInitialized: 'DATAPREP_SET_INITIALIZED',
  setSelectedHeaders: 'DATAPREP_SET_SELECTED_HEADERS',
  setHigherVersion: 'DATAPREP_SET_HIGHER_VERSION',
  setWorkspaceMode: 'DATAPREP_SET_WORKSPACE_MODE',
  enableLoading: 'DATAPREP_ENABLE_LOADING',
  disableLoading: 'DATAPREP_DISABLE_LOADING',
  reset: 'DATAPREP_RESET',
  setError: 'DATAPREP_SET_ERROR',
  setDataError: 'DATAPREP_SET_DATA_ERROR',
  setCLIError: 'DATAPREP_CLI_ERROR',
  dismissError: 'DATAPREP_DISMISS_ERROR',
  setHighlightColumns: 'DATAPREP_SET_HIGHLIGHT_COLUMNS',
  setColumnsInformation: 'DATAPREP_SET_COLUMNS_INFORMATION',
  setWorkspaceList: 'DATAPREP_SET_WORKSPACE_LIST'
};

export default DataPrepActions;
