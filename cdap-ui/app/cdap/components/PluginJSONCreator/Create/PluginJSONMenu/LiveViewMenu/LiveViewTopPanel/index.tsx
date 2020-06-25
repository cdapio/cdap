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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import CollapseLiveViewButton from 'components/PluginJSONCreator/Create/PluginJSONMenu/JSONActionButtons/CollapseLiveViewButton';
import JSONDownloadButton from 'components/PluginJSONCreator/Create/PluginJSONMenu/JSONActionButtons/JSONDownloadButton';
import JSONImportButton from 'components/PluginJSONCreator/Create/PluginJSONMenu/JSONActionButtons/JSONImportButton';
import SwitchLiveViewModeButton from 'components/PluginJSONCreator/Create/PluginJSONMenu/JSONActionButtons/SwitchLiveViewModeButton';
import * as React from 'react';

const styles = (theme): StyleRules => {
  return {
    JSONActionButtons: {
      padding: '0px',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
    },
    liveViewConfig: {
      marginLeft: 'auto',
      display: 'flex',
      flexDirection: 'row',
    },
    buttonTooltip: {
      fontSize: '14px',
      backgroundColor: theme.palette.grey[500],
    },
    currentFilename: {
      position: 'relative',
      margin: '0 auto',
      left: '25px',
      fontFamily: 'Courier New',
    },
  };
};

interface ILiveViewTopPanelProps extends WithStyles<typeof styles> {
  collapseLiveView: () => void;
  onDownloadClick: () => void;
  populateImportResults: (filename: string, fileContent: string) => void;
  JSONFilename: string;
  downloadDisabled: boolean;
  liveViewMode: boolean;
  setLiveViewMode: (liveViewMode: boolean) => void;
}

const LiveViewTopPanelView: React.FC<ILiveViewTopPanelProps> = ({
  classes,
  collapseLiveView,
  onDownloadClick,
  populateImportResults,
  JSONFilename,
  downloadDisabled,
  liveViewMode,
  setLiveViewMode,
}) => {
  return (
    <div className={classes.JSONActionButtons}>
      <JSONImportButton populateImportResults={populateImportResults} />
      <JSONDownloadButton downloadDisabled={downloadDisabled} onDownloadClick={onDownloadClick} />

      <div className={classes.currentFilename}>{JSONFilename}</div>

      <div className={classes.liveViewConfig}>
        <SwitchLiveViewModeButton liveViewMode={liveViewMode} setLiveViewMode={setLiveViewMode} />
        <CollapseLiveViewButton collapseLiveView={collapseLiveView} />
      </div>
    </div>
  );
};

const LiveViewTopPanel = withStyles(styles)(LiveViewTopPanelView);
export default LiveViewTopPanel;
