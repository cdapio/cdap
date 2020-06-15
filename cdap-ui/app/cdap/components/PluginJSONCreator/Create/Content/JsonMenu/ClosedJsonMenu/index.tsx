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
import Drawer from '@material-ui/core/Drawer';
import IconButton from '@material-ui/core/IconButton';
import List from '@material-ui/core/List';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import Tooltip from '@material-ui/core/Tooltip';
import CodeIcon from '@material-ui/icons/Code';
import GetAppIcon from '@material-ui/icons/GetApp';
import { downloadPluginJSON } from 'components/PluginJSONCreator/Create/Content/JsonMenu/utilities';
import { ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const styles = (theme): StyleRules => {
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
    },
    jsonViewerTooltip: {
      fontSize: '14px',
      backgroundColor: theme.palette.grey[500],
    },
  };
};

const ClosedJsonMenuView: React.FC<ICreateContext & WithStyles<typeof styles>> = (
  widgetJSONData
) => {
  const { classes, pluginName, pluginType, jsonView, setJsonView } = widgetJSONData;
  const downloadDisabled = pluginName.length === 0 || pluginType.length === 0;
  return (
    <div>
      <Drawer
        open={!jsonView}
        variant="persistent"
        className={classes.closedJsonMenu}
        anchor="right"
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
                  ? 'Download is disabled until the required fields are filled'
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
    </div>
  );
};

const ClosedJsonMenu = withStyles(styles)(ClosedJsonMenuView);
export default ClosedJsonMenu;
