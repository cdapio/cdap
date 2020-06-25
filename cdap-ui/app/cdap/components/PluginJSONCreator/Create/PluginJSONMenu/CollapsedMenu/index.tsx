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

import Divider from '@material-ui/core/Divider';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import ExpandLiveViewButton from 'components/PluginJSONCreator/Create/PluginJSONMenu/JSONActionButtons/ExpandLiveViewButtton';
import JSONDownloadButton from 'components/PluginJSONCreator/Create/PluginJSONMenu/JSONActionButtons/JSONDownloadButton';
import JSONImportButton from 'components/PluginJSONCreator/Create/PluginJSONMenu/JSONActionButtons/JSONImportButton';
import * as React from 'react';

const styles = (theme): StyleRules => {
  return {
    JSONActionButtons: {
      padding: '15px',
      flexDirection: 'column',
    },
    JSONActionButton: {
      margin: '5px',
    },
    buttonTooltip: {
      fontSize: '14px',
      backgroundColor: theme.palette.grey[500],
    },
  };
};

interface ICollapsedMenuProps extends WithStyles<typeof styles> {
  expandLiveView: () => void;
  onDownloadClick: () => void;
  populateImportResults: (filename: string, fileContent: string) => void;
  downloadDisabled: boolean;
}

const CollapsedMenuView: React.FC<ICollapsedMenuProps> = ({
  classes,
  expandLiveView,
  onDownloadClick,
  populateImportResults,
  downloadDisabled,
}) => {
  return (
    <div>
      <div className={classes.JSONActionButtons}>
        <div className={classes.JSONActionButton}>
          <ExpandLiveViewButton expandLiveView={expandLiveView} />
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

const CollapsedMenu = withStyles(styles)(CollapsedMenuView);
export default CollapsedMenu;
