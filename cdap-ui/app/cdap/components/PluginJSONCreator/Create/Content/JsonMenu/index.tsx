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

import Drawer from '@material-ui/core/Drawer';
import List from '@material-ui/core/List';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import Alert from 'components/Alert';
import If from 'components/If';
import ClosedJsonMenu from 'components/PluginJSONCreator/Create/Content/JsonMenu/ClosedJsonMenu';
import LiveViewer from 'components/PluginJSONCreator/Create/Content/JsonMenu/LiveViewer';
import {
  downloadPluginJSON,
  getJSONConfig,
  parsePluginJSON,
} from 'components/PluginJSONCreator/Create/Content/JsonMenu/utilities';
import { ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const LIVE_VIEWER_WIDTH = '600px';

const styles = (theme): StyleRules => {
  return {
    mainMenu: {
      borderTop: `1px solid ${theme.palette.grey['500']}`,
      paddingTop: theme.Spacing(1),
      paddingBottom: theme.Spacing(1),
    },
    toolbar: {
      minHeight: '48px',
    },
    closedJsonMenu: {
      zIndex: 1000, // lower than '1061', which is Alert component's z-index
    },
    closedJsonMenuPaper: {
      zIndex: 1000, // lower than '1061', which is Alert component's z-index
      backgroundColor: theme.palette.white[50],
    },
    liveViewer: {
      zIndex: 1000, // lower than '1061', which is Alert component's z-index
      width: LIVE_VIEWER_WIDTH,
    },
    liveViewerPaper: {
      zIndex: 1000, // lower than '1061', which is Alert component's z-index
      width: LIVE_VIEWER_WIDTH,
      backgroundColor: theme.palette.white[50],
    },
  };
};

export enum JSONStatusMessage {
  Normal = '',
  Success = 'SUCCESS',
  Fail = 'FAIL',
}

const JsonMenuView: React.FC<ICreateContext & WithStyles<typeof styles>> = (widgetJSONProps) => {
  const {
    classes,
    pluginName,
    pluginType,
    liveView,
    setLiveView,
    setPluginState,
    JSONStatus,
    setJSONStatus,
  } = widgetJSONProps;
  const [JSONErrorMessage, setJSONErrorMessage] = React.useState('');

  const JSONFilename = `${pluginName ? pluginName : '<PluginName>'}-${
    pluginType ? pluginType : '<PluginType>'
  }.json`;

  const downloadDisabled =
    !pluginName || pluginName.length === 0 || !pluginType || pluginType.length === 0;

  const onDownloadClick = () => {
    downloadPluginJSON(widgetJSONProps);
  };

  const populateImportResults = (filename: string, fileContent: string) => {
    try {
      const pluginJSON = JSON.parse(fileContent);

      const {
        basicPluginInfo,
        newConfigurationGroups,
        newGroupToInfo,
        newGroupToWidgets,
        newWidgetInfo,
        newWidgetToAttributes,
        newOutputName,
        newFilters,
        newFilterToName,
        newFilterToCondition,
        newFilterToShowList,
        newShowToInfo,
      } = parsePluginJSON(filename, pluginJSON);

      setPluginState({
        basicPluginInfo,
        configurationGroups: newConfigurationGroups,
        groupToInfo: newGroupToInfo,
        groupToWidgets: newGroupToWidgets,
        widgetInfo: newWidgetInfo,
        widgetToAttributes: newWidgetToAttributes,
        outputName: newOutputName,
        filters: newFilters,
        filterToName: newFilterToName,
        filterToCondition: newFilterToCondition,
        filterToShowList: newFilterToShowList,
        showToInfo: newShowToInfo,
      });

      setJSONStatus(JSONStatusMessage.Success);
      setJSONErrorMessage(null);
    } catch (e) {
      setJSONStatus(JSONStatusMessage.Fail);
      setJSONErrorMessage(`${e.name}: ${e.message}`);
    }
  };

  const expandLiveView = () => {
    setLiveView(true);
  };

  const collapseLiveView = () => {
    setLiveView(false);
  };

  const resetJSONStatus = () => {
    // When alert closes, reset JSONStatus
    setJSONStatus(JSONStatusMessage.Normal);
  };

  return (
    <div>
      <Drawer
        open={true}
        variant="persistent"
        className={liveView ? classes.liveViewer : classes.closedJsonMenu}
        anchor="right"
        ModalProps={{
          keepMounted: true,
        }}
        classes={{
          paperAnchorRight: liveView ? classes.liveViewerPaper : classes.closedJsonMenuPaper,
        }}
        data-cy="navbar-drawer"
      >
        <div className={classes.toolbar} />
        <List component="nav" dense={true} className={classes.mainMenu}>
          <If condition={liveView}>
            <LiveViewer
              JSONConfig={getJSONConfig(widgetJSONProps)}
              downloadDisabled={downloadDisabled}
              collapseLiveView={collapseLiveView}
              onDownloadClick={onDownloadClick}
              populateImportResults={populateImportResults}
              JSONFilename={JSONFilename}
              JSONStatus={JSONStatus}
            />
          </If>
          <If condition={!liveView}>
            <ClosedJsonMenu
              downloadDisabled={downloadDisabled}
              onDownloadClick={onDownloadClick}
              expandLiveView={expandLiveView}
              populateImportResults={populateImportResults}
            />
          </If>
        </List>
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

const JsonMenu = withStyles(styles)(JsonMenuView);
export default JsonMenu;
