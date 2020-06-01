/*
<<<<<<< HEAD
 * Copyright © 2020 Cask Data, Inc.
=======
 * Copyright © 2018 Cask Data, Inc.
>>>>>>> b68b04d60ed... [CDAP-16863] Show the live JSON view (plugin JSON Creator)
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

<<<<<<< HEAD
import Button from '@material-ui/core/Button';
import Divider from '@material-ui/core/Divider';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import Tooltip from '@material-ui/core/Tooltip';
import CodeIcon from '@material-ui/icons/Code';
import GetAppIcon from '@material-ui/icons/GetApp';
import { JSONStatusMessage } from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import PluginJSONImporter from 'components/PluginJSONCreator/Create/Content/JsonMenu/PluginJsonImporter';
=======
import { Divider, IconButton, Tooltip } from '@material-ui/core';
import Drawer from '@material-ui/core/Drawer';
import List from '@material-ui/core/List';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import CodeIcon from '@material-ui/icons/Code';
import GetAppIcon from '@material-ui/icons/GetApp';
import { downloadPluginJSON } from 'components/PluginJSONCreator/Create/Content/JsonMenu/utilities';
>>>>>>> b68b04d60ed... [CDAP-16863] Show the live JSON view (plugin JSON Creator)
import { ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const styles = (theme): StyleRules => {
  return {
<<<<<<< HEAD
    jsonActionButtons: {
      padding: '15px',
      flexDirection: 'column',
      '& > *': {
        margin: '5px',
      },
    },
    jsonActionButton: {
      margin: '5px',
    },
    liveViewerTooltip: {
=======
    closedJsonMenu: {
      zIndex: theme.zIndex.drawer,
    },
    closedJsonMenuPaper: {
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
    jsonCollapseActionButtons: {
      padding: '15px',
      flexDirection: 'column',
    },
    jsonViewerTooltip: {
>>>>>>> b68b04d60ed... [CDAP-16863] Show the live JSON view (plugin JSON Creator)
      fontSize: '14px',
      backgroundColor: theme.palette.grey[500],
    },
  };
};

<<<<<<< HEAD
const DownloadJSONButton = ({ classes, downloadDisabled, onDownloadClick }) => {
  return (
    <Tooltip
      title={
        downloadDisabled
          ? 'Download is disabled until the required fields are filled in'
          : 'Download Plugin JSON'
      }
      classes={{
        tooltip: classes.liveViewerTooltip,
      }}
    >
      <div>
        <Button disabled={downloadDisabled} onClick={onDownloadClick}>
          <GetAppIcon />
        </Button>
      </div>
    </Tooltip>
  );
};

const ExpandLiveViewButton = ({ classes, expandLiveView }) => {
  return (
    <Tooltip
      title="Open JSON View"
      classes={{
        tooltip: classes.liveViewerTooltip,
      }}
    >
      <Button onClick={expandLiveView}>
        <CodeIcon />
      </Button>
    </Tooltip>
  );
};

interface IClosedJsonMenuProps extends WithStyles<typeof styles>, ICreateContext {
  expandLiveView: () => void;
  onDownloadClick: () => void;
  populateImportResults: (filename: string, fileContent: string) => void;
  JSONStatus: JSONStatusMessage;
  downloadDisabled: boolean;
}

const ClosedJsonMenuView: React.FC<IClosedJsonMenuProps> = ({
  classes,
  expandLiveView,
  onDownloadClick,
  populateImportResults,
  JSONStatus,
  downloadDisabled,
}) => {
  return (
    <div>
      <div className={classes.jsonActionButtons}>
        <div className={classes.jsonActionButton}>
          <ExpandLiveViewButton classes={classes} expandLiveView={expandLiveView} />
        </div>
        <Divider />
        <div className={classes.jsonActionButton}>
          <Tooltip
            classes={{
              tooltip: classes.liveViewerTooltip,
            }}
            title="Import JSON"
          >
            <PluginJSONImporter
              populateImportResults={populateImportResults}
              JSONStatus={JSONStatus}
            />
          </Tooltip>
        </div>
        <div className={classes.jsonActionButton}>
          <DownloadJSONButton
            classes={classes}
            downloadDisabled={downloadDisabled}
            onDownloadClick={onDownloadClick}
          />
        </div>
      </div>
=======
const ClosedJsonMenuView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
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
  const downloadDisabled = pluginName.length === 0 || pluginType.length === 0;
  return (
    <div>
      <Drawer
        open={!jsonView}
        variant="persistent"
        className={classes.closedJsonMenu}
        anchor="right"
        disableEnforceFocus={true}
        ModalProps={{
          keepMounted: true,
        }}
        classes={{
          paper: classes.closedJsonMenuPaper,
        }}
        data-cy="navbar-jsonViewer"
      >
        <div className={classes.toolbar} />
        <List component="nav" dense={true} className={classes.mainMenu}>
          <div className={classes.jsonCollapseActionButtons}>
            <Tooltip
              title="Open JSON View"
              classes={{
                tooltip: classes.jsonViewerTooltip,
              }}
            >
              <IconButton onClick={() => setJsonView(true)}>
                <CodeIcon />
              </IconButton>
            </Tooltip>
            <Divider />

            <Tooltip
              title={
                downloadDisabled
                  ? 'Download is disabled until the required fields are filled in'
                  : 'Download Plugin JSON'
              }
              classes={{
                tooltip: classes.jsonViewerTooltip,
              }}
            >
              <span>
                <IconButton
                  disabled={downloadDisabled}
                  onClick={() => downloadPluginJSON(widgetJSONData)}
                >
                  <GetAppIcon />
                </IconButton>
              </span>
            </Tooltip>
          </div>
        </List>
      </Drawer>
>>>>>>> b68b04d60ed... [CDAP-16863] Show the live JSON view (plugin JSON Creator)
    </div>
  );
};

const ClosedJsonMenu = withStyles(styles)(ClosedJsonMenuView);
export default ClosedJsonMenu;
