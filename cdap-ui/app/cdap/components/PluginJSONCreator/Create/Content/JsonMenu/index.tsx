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
import If from 'components/If';
import ClosedJsonMenu from 'components/PluginJSONCreator/Create/Content/JsonMenu/ClosedJsonMenu';
import JsonLiveViewer from 'components/PluginJSONCreator/Create/Content/JsonMenu/JsonLiveViewer';
import {
  downloadPluginJSON,
  getJSONConfig,
  parsePluginJSON,
} from 'components/PluginJSONCreator/Create/Content/JsonMenu/utilities';
import { ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const JSON_VIEWER_WIDTH = '600px';

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
      zIndex: theme.zIndex.drawer,
    },
    closedJsonMenuPaper: {
      backgroundColor: theme.palette.white[50],
    },
    jsonViewer: {
      zIndex: theme.zIndex.drawer,
      width: JSON_VIEWER_WIDTH,
    },
    jsonViewerPaper: {
      width: JSON_VIEWER_WIDTH,
      backgroundColor: theme.palette.white[50],
    },
  };
};

export enum JSONStatusMessage {
  Pending = '',
  Success = 'SUCCESS',
  Fail = 'FAIL',
}

const JsonMenuView: React.FC<ICreateContext & WithStyles<typeof styles>> = (widgetJSONProps) => {
  const {
    classes,
    pluginName,
    pluginType,
    jsonView,
    setJsonView,
    setPluginState,
  } = widgetJSONProps;

  const [JSONStatus, setJSONStatus] = React.useState(JSONStatusMessage.Pending);
  const [JSONErrorMessage, setJSONErrorMessage] = React.useState('');

  // When JSON error occurs, show the error message for 2 seconds.
  React.useEffect(() => {
    const timer = setTimeout(() => setJSONStatus(JSONStatusMessage.Pending), 2000);

    return () => {
      clearTimeout(timer);
    };
  }, [JSONStatus]);

  const jsonFilename = `${pluginName ? pluginName : '<PluginName>'}-${
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
      } = parsePluginJSON(filename, pluginJSON);

      setPluginState({
        basicPluginInfo,
        configurationGroups: newConfigurationGroups,
        groupToInfo: newGroupToInfo,
        groupToWidgets: newGroupToWidgets,
        widgetInfo: newWidgetInfo,
        widgetToAttributes: newWidgetToAttributes,
        outputName: newOutputName,
      });
      setJSONStatus(JSONStatusMessage.Success);
      setJSONErrorMessage(null);
    } catch (e) {
      setJSONStatus(JSONStatusMessage.Fail);
      setJSONErrorMessage(`${e.name}: ${e.message}`);
    }
  };

  const expandJSONView = () => {
    setJsonView(true);
  };

  const collapseJSONView = () => {
    setJsonView(false);
  };

  return (
    <Drawer
      open={true}
      variant="persistent"
      className={jsonView ? classes.jsonViewer : classes.closedJsonMenu}
      anchor="right"
      ModalProps={{
        keepMounted: true,
      }}
      classes={{
        paper: jsonView ? classes.jsonViewerPaper : classes.closedJsonMenuPaper,
      }}
      data-cy="navbar-drawer"
    >
      <div className={classes.toolbar} />
      <List component="nav" dense={true} className={classes.mainMenu}>
        <If condition={jsonView}>
          <JsonLiveViewer
            JSONConfig={getJSONConfig(widgetJSONProps)}
            downloadDisabled={downloadDisabled}
            collapseJSONView={collapseJSONView}
            onDownloadClick={onDownloadClick}
            populateImportResults={populateImportResults}
            jsonFilename={jsonFilename}
            JSONStatus={JSONStatus}
            JSONErrorMessage={JSONErrorMessage}
          />
        </If>
        <If condition={!jsonView}>
          <ClosedJsonMenu
            downloadDisabled={downloadDisabled}
            onDownloadClick={onDownloadClick}
            expandJSONView={expandJSONView}
            populateImportResults={populateImportResults}
          />
        </If>
      </List>
    </Drawer>
  );
};

const JsonMenu = withStyles(styles)(JsonMenuView);
export default JsonMenu;
