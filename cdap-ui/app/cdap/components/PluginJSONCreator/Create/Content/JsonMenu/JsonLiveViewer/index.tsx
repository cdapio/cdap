/*
 * Copyright © 2018 Cask Data, Inc.
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

import { Button, Tooltip } from '@material-ui/core';
import Drawer from '@material-ui/core/Drawer';
import List from '@material-ui/core/List';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import FullscreenExitIcon from '@material-ui/icons/FullscreenExit';
import SaveAltIcon from '@material-ui/icons/SaveAlt';
import JsonEditorWidget from 'components/AbstractWidget/CodeEditorWidget/JsonEditorWidget';
import {
  downloadPluginJSON,
  getJSONConfig,
} from 'components/PluginJSONCreator/Create/Content/JsonMenu/utilities';
import { ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const JSON_VIEWER_WIDTH = '600px';

const styles = (theme): StyleRules => {
  return {
    jsonViewer: {
      zIndex: theme.zIndex.drawer,
      width: JSON_VIEWER_WIDTH,
    },
    jsonViewerPaper: {
      width: JSON_VIEWER_WIDTH,
      backgroundColor: theme.palette.white[50],
    },
    toolbar: {
      minHeight: '48px',
    },
    mainMenu: {
      borderTop: `1px solid ${theme.palette.grey['500']}`,
      paddingTop: theme.Spacing(1),
      paddingBottom: theme.Spacing(1),
    },
    jsonActionButtons: {
      padding: '5px',
      display: 'flex',
    },
    closeJSONViewerButon: {
      marginLeft: 'auto',
    },
    jsonViewerTooltip: {
      fontSize: '14px',
      backgroundColor: theme.palette.grey[500],
    },
  };
};

const JsonLiveViewerView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  pluginName,
  pluginType,
  displayName,
  emitAlerts,
  emitErrors,
  configurationGroups,
  groupToInfo,
  groupToWidgets,
  widgetToInfo,
  widgetToAttributes,
  jsonView,
  setJsonView,
}) => {
  const widgetJSONData = {
    pluginName,
    pluginType,
    displayName,
    emitAlerts,
    emitErrors,
    configurationGroups,
    groupToInfo,
    groupToWidgets,
    widgetToInfo,
    widgetToAttributes,
  };
  const JSONConfig = getJSONConfig(widgetJSONData);
  const downloadDisabled = pluginName.length === 0 || pluginType.length === 0;
  return (
    <div>
      <Drawer
        open={jsonView}
        variant="persistent"
        className={classes.jsonViewer}
        anchor="right"
        disableEnforceFocus={true}
        ModalProps={{
          keepMounted: true,
        }}
        classes={{
          paper: classes.jsonViewerPaper,
        }}
        data-cy="navbar-drawer"
      >
        <div className={classes.toolbar} />
        <List component="nav" dense={true} className={classes.mainMenu}>
          <div className={classes.jsonActionButtons}>
            <Tooltip
              classes={{
                tooltip: classes.jsonViewerTooltip,
              }}
              title={
                downloadDisabled
                  ? 'Download is disabled until the required fields are filled in'
                  : 'Download Plugin JSON'
              }
            >
              <span>
                <Button
                  disabled={downloadDisabled}
                  onClick={() => downloadPluginJSON(widgetJSONData)}
                >
                  <SaveAltIcon />
                </Button>
              </span>
            </Tooltip>
            <Tooltip
              classes={{
                tooltip: classes.jsonViewerTooltip,
              }}
              title="Close JSON View"
            >
              <Button className={classes.closeJSONViewerButon} onClick={() => setJsonView(false)}>
                <FullscreenExitIcon />
              </Button>
            </Tooltip>
          </div>
          <JsonEditorWidget
            rows={50}
            value={JSON.stringify(JSONConfig, undefined, 4)}
          ></JsonEditorWidget>
        </List>
      </Drawer>
    </div>
  );
};

const JsonLiveViewer = withStyles(styles)(JsonLiveViewerView);
export default JsonLiveViewer;
