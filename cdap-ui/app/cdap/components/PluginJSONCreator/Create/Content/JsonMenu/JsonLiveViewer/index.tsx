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
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import FullscreenExitIcon from '@material-ui/icons/FullscreenExit';
import GetAppIcon from '@material-ui/icons/GetApp';
import JsonEditorWidget from 'components/AbstractWidget/CodeEditorWidget/JsonEditorWidget';
import If from 'components/If';
import { JSONStatusMessage } from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import PluginJSONImporter from 'components/PluginJSONCreator/Create/Content/JsonMenu/PluginJSONImporter';
import { ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const JSON_VIEWER_WIDTH = '600px';

const styles = (theme) => {
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
      padding: '0px',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
    },
    currentFilename: {
      position: 'relative',
      margin: '0 auto',
      left: '25px',
      fontFamily: 'Courier New',
    },
    jsonFailStatus: {
      position: 'relative',
      margin: '0 auto',
      color: theme.palette.red[50],
      fontFamily: 'Courier New',
    },
    jsonSuccessStatus: {
      position: 'relative',
      margin: '0 auto',
      color: theme.palette.blue[50],
      fontFamily: 'Courier New',
    },
    closeJSONViewerButon: {
      marginLeft: 'auto',
      order: '3',
    },
    jsonViewerTooltip: {
      fontSize: '14px',
      backgroundColor: theme.palette.grey[500],
    },
  };
};

const DownloadJSONButton = ({ classes, downloadDisabled, onDownloadClick }) => {
  return (
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
      <Button disabled={downloadDisabled} onClick={onDownloadClick}>
        <GetAppIcon />
      </Button>
    </Tooltip>
  );
};

const CollapseJSONViewButton = ({ classes, collapseJSONView }) => {
  return (
    <Tooltip
      classes={{
        tooltip: classes.jsonViewerTooltip,
      }}
      title="Close JSON View"
    >
      <Button className={classes.closeJSONViewerButon} onClick={collapseJSONView}>
        <FullscreenExitIcon />
      </Button>
    </Tooltip>
  );
};

const JsonLiveViewerView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  collapseJSONView,
  JSONConfig,
  downloadDisabled,
  onDownloadClick,
  populateImportResults,
  filename,
  JSONStatus,
  JSONErrorMessage,
}) => {
  return (
    <div>
      <Drawer
        open={true}
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
              title="Import JSON"
            >
              <PluginJSONImporter
                populateImportResults={populateImportResults}
                JSONStatus={JSONStatus}
              />
            </Tooltip>
            <DownloadJSONButton
              classes={classes}
              downloadDisabled={downloadDisabled}
              onDownloadClick={onDownloadClick}
            />
            <If
              condition={
                JSONStatus !== JSONStatusMessage.Success && JSONStatus !== JSONStatusMessage.Fail
              }
            >
              <div className={classes.currentFilename}>{filename}</div>
            </If>
            <If
              condition={
                JSONStatus === JSONStatusMessage.Success || JSONStatus === JSONStatusMessage.Fail
              }
            >
              <div
                className={
                  JSONStatus === JSONStatusMessage.Success
                    ? classes.jsonSuccessStatus
                    : classes.jsonFailStatus
                }
              >
                {JSONErrorMessage}
              </div>
            </If>

            <CollapseJSONViewButton classes={classes} collapseJSONView={collapseJSONView} />
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
