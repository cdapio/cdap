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

import { Button, Divider, Tooltip } from '@material-ui/core';
import Drawer from '@material-ui/core/Drawer';
import List from '@material-ui/core/List';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import CodeIcon from '@material-ui/icons/Code';
import GetAppIcon from '@material-ui/icons/GetApp';
import PluginJSONImporter from 'components/PluginJSONCreator/Create/Content/JsonMenu/PluginJSONImporter';
import { ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const styles = (theme) => {
  return {
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
      '& > *': {
        margin: '5px',
      },
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

const ExpandJSONViewButton = ({ classes, expandJSONView }) => {
  return (
    <Tooltip
      title="Open JSON View"
      classes={{
        tooltip: classes.jsonViewerTooltip,
      }}
    >
      <Button onClick={expandJSONView}>
        <CodeIcon />
      </Button>
    </Tooltip>
  );
};

const ClosedJsonMenuView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  expandJSONView,
  downloadDisabled,
  onDownloadClick,
  populateImportResults,
  JSONStatus,
}) => {
  return (
    <div>
      <Drawer
        open={true}
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
            <ExpandJSONViewButton classes={classes} expandJSONView={expandJSONView} />
            <Divider />

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
          </div>
        </List>
      </Drawer>
    </div>
  );
};

const ClosedJsonMenu = withStyles(styles)(ClosedJsonMenuView);
export default ClosedJsonMenu;
