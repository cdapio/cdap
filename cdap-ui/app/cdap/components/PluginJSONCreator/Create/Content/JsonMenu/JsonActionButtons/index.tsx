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
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import Tooltip from '@material-ui/core/Tooltip';
import FullscreenExitIcon from '@material-ui/icons/FullscreenExit';
import GetAppIcon from '@material-ui/icons/GetApp';
import { JSONStatusMessage } from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import { LiveViewMode } from 'components/PluginJSONCreator/Create/Content/JsonMenu/LiveViewer';
import PluginJSONImporter from 'components/PluginJSONCreator/Create/Content/JsonMenu/PluginJsonImporter';
import {
  CreateContext,
  createContextConnect,
  ICreateContext,
} from 'components/PluginJSONCreator/CreateContextConnect';
import ToggleSwitch from 'components/ToggleSwitch';
import * as React from 'react';

const styles = (theme): StyleRules => {
  return {
    jsonActionButtons: {
      padding: '0px',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
    },
    liveViewerConfig: {
      marginLeft: 'auto',
      display: 'flex',
      flexDirection: 'row',
    },
    liveViewerTooltip: {
      fontSize: '14px',
      backgroundColor: theme.palette.grey[500],
    },
    currentFilename: {
      position: 'relative',
      margin: '0 auto',
      left: '25px',
    },
  };
};

const DownloadJSONButton = ({ classes, downloadDisabled, onDownloadClick }) => {
  return (
    <Tooltip
      title={
        downloadDisabled
          ? 'Download is disabled until the required fields are filled'
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

const CollapseLiveViewButton = ({ classes, collapseLiveView }) => {
  return (
    <Tooltip
      classes={{
        tooltip: classes.liveViewerTooltip,
      }}
      title="Close JSON View"
    >
      <Button onClick={collapseLiveView}>
        <FullscreenExitIcon />
      </Button>
    </Tooltip>
  );
};

const LiveViewModeSwitcher = ({ liveViewMode, setLiveViewMode }) => {
  return (
    <div>
      <ToggleSwitch
        isOn={liveViewMode === LiveViewMode.ConfigurationGroupsView}
        onToggle={() => {
          setLiveViewMode(
            liveViewMode === LiveViewMode.JSONView
              ? LiveViewMode.ConfigurationGroupsView
              : LiveViewMode.JSONView
          );
        }}
      />
    </div>
  );
};

interface IJsonActionButtonsProps extends WithStyles<typeof styles>, ICreateContext {
  collapseLiveView: () => void;
  onDownloadClick: () => void;
  populateImportResults: (filename: string, fileContent: string) => void;
  JSONFilename: string;
  JSONStatus: JSONStatusMessage;
  downloadDisabled: boolean;
  JSONErrorMessage: string;
  liveViewMode: boolean;
  setLiveViewMode: (liveViewMode: boolean) => void;
}

const JsonActionButtonsView: React.FC<IJsonActionButtonsProps> = ({
  classes,
  collapseLiveView,
  onDownloadClick,
  populateImportResults,
  JSONFilename,
  JSONStatus,
  downloadDisabled,
  liveViewMode,
  setLiveViewMode,
}) => {
  return (
    <div className={classes.jsonActionButtons}>
      <Tooltip
        classes={{
          tooltip: classes.liveViewerTooltip,
        }}
        title="Import JSON"
      >
        <PluginJSONImporter populateImportResults={populateImportResults} JSONStatus={JSONStatus} />
      </Tooltip>
      <DownloadJSONButton
        classes={classes}
        downloadDisabled={downloadDisabled}
        onDownloadClick={onDownloadClick}
      />

      <pre className={classes.currentFilename}>{JSONFilename}</pre>

      <div className={classes.liveViewerConfig}>
        <LiveViewModeSwitcher liveViewMode={liveViewMode} setLiveViewMode={setLiveViewMode} />
        <CollapseLiveViewButton classes={classes} collapseLiveView={collapseLiveView} />
      </div>
    </div>
  );
};

const StyledJsonActionButtons = withStyles(styles)(JsonActionButtonsView);
const JsonActionButtons = createContextConnect(CreateContext, StyledJsonActionButtons);
export default JsonActionButtons;
