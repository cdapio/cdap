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

import * as React from 'react';

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import Divider from '@material-ui/core/Divider';
import JSONDownloadButton from 'components/PluginJSONCreator/Create/PluginJSONMenu/JSONActionButtons/JSONDownloadButton';
import JSONImportButton from 'components/PluginJSONCreator/Create/PluginJSONMenu/JSONActionButtons/JSONImportButton';
import LiveConfigurationGroupButton from 'components/PluginJSONCreator/Create/PluginJSONMenu/JSONActionButtons/LiveConfigurationGroupButton';
import LiveJSONButton from 'components/PluginJSONCreator/Create/PluginJSONMenu/JSONActionButtons/LiveJSONButton';
import { LiveViewMode } from 'components/PluginJSONCreator/Create/PluginJSONMenu';
import classnames from 'classnames';

const styles = (theme): StyleRules => {
  return {
    JSONActionButtons: {
      flexDirection: 'column',
      borderRight: `1px solid ${theme.palette.divider}`,
      height: 'calc(100vh - 48px)', // height excluding header
    },
    JSONActionButton: {
      padding: '5px',
    },
    buttonTooltip: {
      fontSize: '13px',
      backgroundColor: theme.palette.grey[50],
    },
    expandedButtons: {
      position: 'absolute',
      top: '53px', // considering footer
      left: '0',
    },
  };
};

interface IJSONActionButtonsProps extends WithStyles<typeof styles> {
  liveViewMode: LiveViewMode;
  switchToJSONView: () => void;
  switchToConfigurationGroupsView: () => void;
  onDownloadClick: () => void;
  populateImportResults: (filename: string, fileContent: string) => void;
  downloadDisabled: boolean;
}

const JSONActionButtonsView: React.FC<IJSONActionButtonsProps> = ({
  classes,
  liveViewMode,
  switchToJSONView,
  switchToConfigurationGroupsView,
  onDownloadClick,
  populateImportResults,
  downloadDisabled,
}) => {
  return (
    <div>
      <div
        className={classnames(classes.JSONActionButtons, {
          [classes.expandedButtons]: liveViewMode !== LiveViewMode.None,
        })}
      >
        <div className={classes.JSONActionButton}>
          <LiveJSONButton liveViewMode={liveViewMode} switchToJSONView={switchToJSONView} />
        </div>
        <div className={classes.JSONActionButton}>
          <LiveConfigurationGroupButton
            liveViewMode={liveViewMode}
            switchToConfigurationGroupsView={switchToConfigurationGroupsView}
          />
        </div>
        <Divider />
        <div className={classes.JSONActionButton}>
          <JSONImportButton populateImportResults={populateImportResults} />
        </div>
        <div className={classes.JSONActionButton}>
          <JSONDownloadButton
            downloadDisabled={downloadDisabled}
            onDownloadClick={onDownloadClick}
          />
        </div>
      </div>
    </div>
  );
};

const JSONActionButtons = withStyles(styles)(JSONActionButtonsView);
export default JSONActionButtons;
