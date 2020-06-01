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

import Button from '@material-ui/core/Button';
import Drawer from '@material-ui/core/Drawer';
import List from '@material-ui/core/List';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import Tooltip from '@material-ui/core/Tooltip';
import FullscreenExitIcon from '@material-ui/icons/FullscreenExit';
<<<<<<< HEAD
import SaveAltIcon from '@material-ui/icons/SaveAlt';
import {
  downloadPluginJSON,
  getJSONConfig,
} from 'components/PluginJSONCreator/Create/Content/JsonMenu/utilities';
=======
import GetAppIcon from '@material-ui/icons/GetApp';
import JsonEditorWidget from 'components/AbstractWidget/CodeEditorWidget/JsonEditorWidget';
import If from 'components/If';
import { JSONStatusMessage } from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import PluginJSONImporter from 'components/PluginJSONCreator/Create/Content/JsonMenu/PluginJsonImporter';
>>>>>>> 797996d7ff1... [CDAP-16874] Importing existing plugin JSON file (plugin JSON Creator)
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
    },
    jsonViewerTooltip: {
      fontSize: '14px',
      backgroundColor: theme.palette.grey[500],
    },
    jsonLiveCode: {
      padding: '14px',
    },
  };
};

<<<<<<< HEAD
const JsonLiveViewerView: React.FC<ICreateContext & WithStyles<typeof styles>> = (
  widgetJSONData
) => {
  const { classes, pluginName, pluginType, jsonView, setJsonView } = widgetJSONData;
  const JSONConfig = getJSONConfig(widgetJSONData);
  const downloadDisabled = pluginName.length === 0 || pluginType.length === 0;
=======
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

interface IJsonLiveViewerProps extends WithStyles<typeof styles>, ICreateContext {
  JSONConfig: any;
  collapseJSONView: () => void;
  onDownloadClick: () => void;
  populateImportResults: (filename: string, fileContent: string) => void;
  jsonFilename: string;
  JSONStatus: JSONStatusMessage;
  downloadDisabled: boolean;
  JSONErrorMessage: string;
}

const JsonLiveViewerView: React.FC<IJsonLiveViewerProps> = ({
  classes,
  JSONConfig,
  collapseJSONView,
  onDownloadClick,
  populateImportResults,
  jsonFilename,
  JSONStatus,
  downloadDisabled,
  JSONErrorMessage,
}) => {
>>>>>>> 797996d7ff1... [CDAP-16874] Importing existing plugin JSON file (plugin JSON Creator)
  return (
    <div>
      <Drawer
        open={true}
        variant="persistent"
        className={classes.jsonViewer}
        anchor="right"
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
              <div className={classes.currentFilename}>{jsonFilename}</div>
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
          <div className={classes.jsonLiveCode}>
            <pre>{JSON.stringify(JSONConfig, undefined, 2)}</pre>
          </div>
        </List>
      </Drawer>
    </div>
  );
};

const JsonLiveViewer = withStyles(styles)(JsonLiveViewerView);
export default JsonLiveViewer;
