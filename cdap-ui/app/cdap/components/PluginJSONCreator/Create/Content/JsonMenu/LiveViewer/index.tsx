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
import { JSONStatusMessage } from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import JsonActionButtons from 'components/PluginJSONCreator/Create/Content/JsonMenu/JsonActionButtons';
import LiveConfigurationGroup from 'components/PluginJSONCreator/Create/Content/JsonMenu/LiveViewer/LiveConfigurationGroup';
import * as React from 'react';

export enum LiveViewMode {
  JSONView = 'JSON_VIEW',
  ConfigurationGroupsView = 'CONFIGURATION_GROUPS',
}

const LIVE_VIEWER_WIDTH = '600px';

const styles = (theme): StyleRules => {
  return {
    liveViewer: {
      zIndex: theme.zIndex.drawer,
      width: LIVE_VIEWER_WIDTH,
    },
    liveViewerPaper: {
      width: LIVE_VIEWER_WIDTH,
      backgroundColor: theme.palette.white[50],
    },
    toolbar: {
      minHeight: '48px',
    },
    mainMenu: {
      width: '100%',
      height: '100%',
      borderTop: `1px solid ${theme.palette.grey['500']}`,
      paddingTop: theme.Spacing(1),
      paddingBottom: theme.Spacing(1),
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
    liveViewLoad: {
      maxWidth: '50%',
    },
    jsonLiveCode: {
      padding: '14px',
    },
  };
};

interface ILiveViewerProps extends WithStyles<typeof styles> {
  JSONOutput: any;
  collapseLiveView: () => void;
  onDownloadClick: () => void;
  populateImportResults: (filename: string, fileContent: string) => void;
  JSONFilename: string;
  JSONStatus: JSONStatusMessage;
  downloadDisabled: boolean;
  JSONErrorMessage: string;
}

const LiveViewerView: React.FC<ILiveViewerProps> = ({
  classes,
  JSONOutput,
  collapseLiveView,
  onDownloadClick,
  populateImportResults,
  JSONFilename,
  JSONStatus,
  downloadDisabled,
  JSONErrorMessage,
}) => {
  const [liveViewMode, setLiveViewMode] = React.useState(LiveViewMode.JSONView);

  return (
    <div>
      <Drawer
        open={true}
        variant="persistent"
        className={classes.liveViewer}
        anchor="right"
        ModalProps={{
          keepMounted: true,
        }}
        classes={{
          paper: classes.liveViewerPaper,
        }}
        data-cy="navbar-drawer"
      >
        <div className={classes.toolbar} />
        <JsonActionButtons
          populateImportResults={populateImportResults}
          JSONStatus={JSONStatus}
          downloadDisabled={downloadDisabled}
          onDownloadClick={onDownloadClick}
          JSONFilename={JSONFilename}
          JSONErrorMessage={JSONErrorMessage}
          liveViewMode={liveViewMode}
          setLiveViewMode={setLiveViewMode}
          collapseLiveView={collapseLiveView}
        />
        <List component="nav" dense={true} className={classes.mainMenu}>
          <If condition={liveViewMode === LiveViewMode.JSONView}>
            <div className={classes.jsonLiveCode}>
              <pre>{JSON.stringify(JSONOutput, undefined, 2)}</pre>
            </div>
          </If>
          <If condition={liveViewMode === LiveViewMode.ConfigurationGroupsView}>
            <LiveConfigurationGroup JSONOutput={JSONOutput} />
          </If>
        </List>
      </Drawer>
    </div>
  );
};

const LiveViewer = withStyles(styles)(LiveViewerView);
export default LiveViewer;
