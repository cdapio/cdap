/*
 * Copyright © 2020 Cask Data, Inc.
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

import * as React from 'react';

import {
  downloadPluginJSON,
  getJSONOutput,
  parsePluginJSON,
} from 'components/PluginJSONCreator/utilities';
import {
  useAppInternalState,
  useConfigurationGroupState,
  useFilterState,
  useOutputState,
  usePluginInfoState,
  useWidgetState,
} from 'components/PluginJSONCreator/Create';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import Alert from 'components/Alert';
import Drawer from '@material-ui/core/Drawer';
import If from 'components/If';
import JSONActionButtons from 'components/PluginJSONCreator/Create/PluginJSONMenu/JSONActionButtons';
import { JSONStatusMessage } from 'components/PluginJSONCreator/constants';
import LiveView from 'components/PluginJSONCreator/Create/PluginJSONMenu/LiveView';

export enum LiveViewMode {
  JSONView = 'JSON_VIEW',
  ConfigurationGroupsView = 'CONFIGURATION_GROUPS',
  None = 'NONE',
}

const LIVE_VIEWER_WIDTH = '550px';

const styles = (theme): StyleRules => {
  return {
    mainMenu: {
      display: 'grid',
      gridTemplateColumns: '1fr 9fr',
      width: '100%',
      height: '100%',
      borderTop: `1px solid ${theme.palette.grey['500']}`,
    },
    jsonActionButtons: {
      margin: 'auto',
      padding: '0',
    },
    liveView: {
      width: '100%',
    },
    toolbar: {
      minHeight: '48px',
    },
    closedMenu: {
      zIndex: 1000, // lower than '1061', which is Alert component's z-index
    },
    closedMenuPaper: {
      zIndex: 1000, // lower than '1061', which is Alert component's z-index
      backgroundColor: theme.palette.white[50],
    },
    liveViewer: {
      width: LIVE_VIEWER_WIDTH,
      zIndex: 1000, // lower than '1061', which is Alert component's z-index
    },
    liveViewerPaper: {
      width: LIVE_VIEWER_WIDTH,
      zIndex: 1000, // lower than '1061', which is Alert component's z-index
      backgroundColor: theme.palette.white[50],
    },
  };
};

interface IPluginJSONMenuProps extends WithStyles<typeof styles> {
  uploadedFile: { filename: string; fileContent: string };
}

const PluginJSONMenuView: React.FC<IPluginJSONMenuProps> = ({ classes, uploadedFile }) => {
  const {
    pluginName,
    setPluginName,
    pluginType,
    setPluginType,
    displayName,
    setDisplayName,
    emitAlerts,
    setEmitAlerts,
    emitErrors,
    setEmitErrors,
  } = usePluginInfoState();

  const {
    configurationGroups,
    setConfigurationGroups,
    groupToInfo,
    setGroupToInfo,
  } = useConfigurationGroupState();

  const {
    groupToWidgets,
    setGroupToWidgets,
    widgetInfo,
    setWidgetInfo,
    widgetToAttributes,
    setWidgetToAttributes,
  } = useWidgetState();

  const {
    outputName,
    setOutputName,
    outputWidgetType,
    setOutputWidgetType,
    schemaTypes,
    setSchemaTypes,
    schemaDefaultType,
    setSchemaDefaultType,
    schema,
    setSchema,
  } = useOutputState();

  const {
    filters,
    setFilters,
    filterToName,
    setFilterToName,
    filterToCondition,
    setFilterToCondition,
    filterToShowlist,
    setFilterToShowlist,
    showToInfo,
    setShowToInfo,
  } = useFilterState();

  const { JSONStatus, setJSONStatus } = useAppInternalState();

  const [liveViewMode, setLiveViewMode] = React.useState(LiveViewMode.ConfigurationGroupsView);

  // In the user drag-and-drops a file, it should populate the UI with the file content.
  React.useEffect(() => {
    if (uploadedFile) {
      populateImportResults(uploadedFile.filename, uploadedFile.fileContent);
    }
  }, [uploadedFile]);

  // Compilation of all the data that goes into JSON.
  const widgetData = {
    pluginName,
    pluginType,
    displayName,
    emitAlerts,
    emitErrors,
    configurationGroups,
    setConfigurationGroups,
    groupToInfo,
    setGroupToInfo,
    groupToWidgets,
    widgetInfo,
    widgetToAttributes,
    outputName,
    outputWidgetType,
    schemaTypes,
    schemaDefaultType,
    schema,
    filters,
    filterToName,
    filterToCondition,
    filterToShowlist,
    showToInfo,
  };

  const updateEntireUI = (newWidgetData) => {
    setPluginName(newWidgetData.pluginName);
    setPluginType(newWidgetData.pluginType);
    setDisplayName(newWidgetData.displayName);
    setEmitAlerts(newWidgetData.emitAlerts);
    setEmitErrors(newWidgetData.emitErrors);
    setConfigurationGroups(newWidgetData.configurationGroups);
    setGroupToInfo(newWidgetData.groupToInfo);
    setGroupToWidgets(newWidgetData.groupToWidgets);
    setWidgetInfo(newWidgetData.widgetInfo);
    setWidgetToAttributes(newWidgetData.widgetToAttributes);
    setOutputName(newWidgetData.outputName);
    setOutputWidgetType(newWidgetData.outputWidgetType);
    setSchemaTypes(newWidgetData.schemaTypes);
    setSchemaDefaultType(newWidgetData.schemaDefaultType);
    setSchema(newWidgetData.schema);
    setFilters(newWidgetData.filters);
    setFilterToName(newWidgetData.filterToName);
    setFilterToCondition(newWidgetData.filterToCondition);
    setFilterToShowlist(newWidgetData.filterToShowlist);
    setShowToInfo(newWidgetData.showToInfo);
  };

  const populateImportResults = (filename: string, fileContent: string) => {
    try {
      const pluginJSON = JSON.parse(fileContent);

      const newWidgetData = parsePluginJSON(filename, pluginJSON);

      setJSONStatus(JSONStatusMessage.Success);
      setJSONErrorMessage(null);

      updateEntireUI(newWidgetData);
    } catch (e) {
      setJSONStatus(JSONStatusMessage.Fail);
      setJSONErrorMessage(`${e.name}: ${e.message}`);
    }
  };

  const switchToJSONView = () => {
    setLiveViewMode(LiveViewMode.JSONView);
  };

  const switchToConfigurationGroupsView = () => {
    setLiveViewMode(LiveViewMode.ConfigurationGroupsView);
  };

  const collapseLiveView = () => {
    setLiveViewMode(LiveViewMode.None);
  };

  // When an error Alert component is closed, reset JSONStatus back to Normal
  const resetJSONStatus = () => {
    setJSONStatus(JSONStatusMessage.Normal);
  };

  const onDownloadClick = () => {
    downloadPluginJSON(widgetData);
  };

  const [JSONErrorMessage, setJSONErrorMessage] = React.useState('');

  const [JSONOutput, setJSONOutput] = React.useState(null);

  const JSONFilename = `${pluginName ? pluginName : '<PluginName>'}-${
    pluginType ? pluginType : '<PluginType>'
  }.json`;

  const downloadDisabled =
    !pluginName || pluginName.length === 0 || !pluginType || pluginType.length === 0;

  const isLiveView = liveViewMode !== LiveViewMode.None;

  React.useEffect(() => {
    if (JSONStatus === JSONStatusMessage.Normal) {
      setJSONOutput(getJSONOutput(widgetData));
    }
  }, [JSONStatus, ...Object.values(widgetData)]);

  return (
    <div>
      <Drawer
        open={true}
        variant="persistent"
        className={isLiveView ? classes.liveViewer : classes.closedMenu}
        anchor="right"
        ModalProps={{
          keepMounted: true,
        }}
        classes={{
          paperAnchorRight: isLiveView ? classes.liveViewerPaper : classes.closedMenuPaper,
        }}
        data-cy="navbar-drawer"
      >
        <div className={classes.toolbar} />
        <If condition={isLiveView}>
          <div className={classes.mainMenu}>
            <div className={classes.jsonActionButtons}>
              <JSONActionButtons
                downloadDisabled={downloadDisabled}
                onDownloadClick={onDownloadClick}
                populateImportResults={populateImportResults}
                liveViewMode={liveViewMode}
                switchToJSONView={switchToJSONView}
                switchToConfigurationGroupsView={switchToConfigurationGroupsView}
              />
            </div>
            <div className={classes.liveView}>
              <LiveView
                liveViewMode={liveViewMode}
                JSONFilename={JSONFilename}
                JSONOutput={JSONOutput}
                collapseLiveView={collapseLiveView}
              />
            </div>
          </div>
        </If>
        <If condition={!isLiveView}>
          <div className={classes.jsonActionButtons}>
            <JSONActionButtons
              downloadDisabled={downloadDisabled}
              onDownloadClick={onDownloadClick}
              populateImportResults={populateImportResults}
              liveViewMode={liveViewMode}
              switchToJSONView={switchToJSONView}
              switchToConfigurationGroupsView={switchToConfigurationGroupsView}
            />
          </div>
        </If>
      </Drawer>

      <Alert
        message={JSONErrorMessage}
        showAlert={JSONStatus === JSONStatusMessage.Fail}
        type="error"
        onClose={resetJSONStatus}
      />
    </div>
  );
};

const PluginJSONMenu = withStyles(styles)(PluginJSONMenuView);
export default React.memo(PluginJSONMenu);
