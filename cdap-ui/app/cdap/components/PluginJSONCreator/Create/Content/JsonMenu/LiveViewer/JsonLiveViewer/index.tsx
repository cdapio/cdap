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
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import FullscreenExitIcon from '@material-ui/icons/FullscreenExit';
import GetAppIcon from '@material-ui/icons/GetApp';
import JsonEditorWidget from 'components/AbstractWidget/CodeEditorWidget/JsonEditorWidget';
import { ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const JSON_VIEWER_WIDTH = '600px';

const styles = (theme) => {
  return {
    liveViewer: {
      zIndex: theme.zIndex.drawer,
      width: JSON_VIEWER_WIDTH,
    },
    liveViewerPaper: {
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
    liveViewerTooltip: {
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
        tooltip: classes.liveViewerTooltip,
      }}
    >
      <Button disabled={downloadDisabled} onClick={onDownloadClick}>
        <GetAppIcon />
      </Button>
    </Tooltip>
  );
};

const CollapseJSONViewButton = ({ classes, collapseLiveView }) => {
  return (
    <Tooltip
      classes={{
        tooltip: classes.liveViewerTooltip,
      }}
      title="Close JSON View"
    >
      <Button className={classes.closeJSONViewerButon} onClick={collapseLiveView}>
        <FullscreenExitIcon />
      </Button>
    </Tooltip>
  );
};

const JsonLiveViewerView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  JSONConfig,
}) => {
  return (
    <div>
      <JsonEditorWidget
        rows={50}
        value={JSON.stringify(JSONConfig, undefined, 4)}
      ></JsonEditorWidget>
    </div>
  );
};

const JsonLiveViewer = withStyles(styles)(JsonLiveViewerView);
export default JsonLiveViewer;
