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

import { Button, Divider, Grid, IconButton } from '@material-ui/core';
import Drawer from '@material-ui/core/Drawer';
import List from '@material-ui/core/List';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import CloseIcon from '@material-ui/icons/Close';
import CodeIcon from '@material-ui/icons/Code';
import GetAppIcon from '@material-ui/icons/GetApp';
import SaveAltIcon from '@material-ui/icons/SaveAlt';
import JsonEditorWidget from 'components/AbstractWidget/CodeEditorWidget/JsonEditorWidget';
import { INamespaceLinkContext } from 'components/AppHeader/NamespaceLinkContext';
import { createContextConnect, IWidgetInfo } from 'components/PluginJSONCreator/Create';
import fileDownload from 'js-file-download';
import * as React from 'react';

const JSON_VIEWER_WIDTH = '600px';
const DRAWER_WIDTH = '200px';

export const appDrawerListItemTextStyles = (theme) => ({
  fontWeight: 400,
  fontSize: '1.1rem',
  paddingLeft: theme.Spacing(3),
  paddingRight: theme.Spacing(3),
  lineHeight: 1.5,
});

export const appDrawerListItemStyles = (theme) => ({
  padding: `${theme.Spacing(1)}px ${theme.Spacing(4)}px`,
  '&:hover': {
    backgroundColor: theme.palette.grey['500'],
    color: theme.palette.grey['100'],
  },
});

const styles = (theme) => {
  return {
    jsonViewer: {
      zIndex: theme.zIndex.drawer,
      width: JSON_VIEWER_WIDTH,
    },
    drawer: {
      zIndex: theme.zIndex.drawer,
      width: DRAWER_WIDTH,
    },
    jsonViewerPaper: {
      width: JSON_VIEWER_WIDTH,
      backgroundColor: theme.palette.grey['700'],
    },
    drawerPaper: {
      backgroundColor: theme.palette.grey['700'],
    },
    closeJSONViewerButon: {
      float: 'right',
    },
    listItemText: appDrawerListItemTextStyles(theme),
    toolbar: {
      minHeight: '48px',
    },
    mainMenu: {
      borderTop: `1px solid ${theme.palette.grey['500']}`,
      paddingTop: theme.Spacing(1),
      paddingBottom: theme.Spacing(1),
    },
    namespaceAdminMenu: {
      // WUT TS?
      position: 'absolute' as 'absolute',
      bottom: '0px',
      width: '100%',
      borderTop: `1px solid ${theme.palette.grey['500']}`,
    },
  };
};

interface IJsonLiveViewerProps {
  open: boolean;
  onClose: () => void;
  context: INamespaceLinkContext;
}

const JsonLiveViewerView: React.FC<IJsonLiveViewerProps & WithStyles<typeof styles>> = ({
  classes,
  displayName,
  configurationGroups,
  groupToInfo,
  groupToWidgets,
  widgetToInfo,
  widgetToAttributes,
  filters,
  filterToName,
  filterToCondition,
  filterToShowList,
  showToInfo,
  outputName,
  open,
  onClose,
}) => {
  function getJSONConfig() {
    const configurationGroupsData = configurationGroups.map((groupID: string) => {
      const groupLabel = groupToInfo[groupID].label;
      const widgetData = groupToWidgets[groupID].map((widgetID: string) => {
        const widgetInfo: IWidgetInfo = widgetToInfo[widgetID];

        const widgetType = widgetInfo.widgetType;
        const widgetAttributes = widgetToAttributes[widgetID];
        return {
          'widget-type': widgetInfo.widgetType,
          label: widgetInfo.label,
          name: widgetInfo.name,
          ...(widgetInfo.widgetCategory && { 'widget-category': widgetInfo.widgetCategory }),
          ...(widgetAttributes &&
            Object.keys(widgetAttributes).length > 0 && {
              'widget-attributes': widgetAttributes,
            }),
        };
      });
      return {
        label: groupLabel,
        properties: widgetData,
      };
    });

    const outputsData = {
      ...(outputName && { name: outputName }),
    };

    const filtersData = filters.map((filterID) => {
      const filterToShowListData = filterToShowList[filterID].map((showID) => {
        return {
          name: showToInfo[showID].name,
          ...(showToInfo[showID].type && {
            type: showToInfo[showID].type,
          }),
        };
      });
      return {
        name: filterToName[filterID],
        condition: filterToCondition[filterID],
        show: filterToShowListData,
      };
    });

    const config = {
      metadata: {
        'spec-version': 'TODO',
      },
      'display-name': displayName,
      'configuration-groups': configurationGroupsData,
      ...(outputsData &&
        Object.keys(outputsData).length > 0 && {
          outputs: outputsData,
        }),
      ...(filtersData &&
        Object.keys(filtersData).length > 0 && {
          filters: filtersData,
        }),
      'jump-config': '{TODO}',
    };

    return config;
  }

  function downloadPluginJSON() {
    fileDownload(JSON.stringify(JSONConfig, undefined, 4), 'sample.json');
  }

  const JSONConfig = getJSONConfig();

  const [isJSONViewOpen, setIsJSONViewOpen] = React.useState(true);

  return (
    <div>
      <Drawer
        open={isJSONViewOpen}
        variant="persistent"
        onClose={onClose}
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
          <Button variant="contained" color="primary" onClick={downloadPluginJSON}>
            <Grid container direction="row" alignItems="center">
              <Grid item>
                <SaveAltIcon style={{ marginRight: '5px' }} />
              </Grid>
              <Grid item>Download</Grid>
            </Grid>
          </Button>
          <IconButton
            className={classes.closeJSONViewerButon}
            onClick={() => setIsJSONViewOpen(false)}
          >
            <CloseIcon />
          </IconButton>
          <JsonEditorWidget
            rows={50}
            value={JSON.stringify(JSONConfig, undefined, 4)}
          ></JsonEditorWidget>
        </List>
      </Drawer>

      <Drawer
        open={!isJSONViewOpen}
        variant="persistent"
        onClose={onClose}
        className={classes.drawer}
        anchor="right"
        disableEnforceFocus={true}
        ModalProps={{
          keepMounted: true,
        }}
        classes={{
          paper: classes.drawerPaper,
        }}
        data-cy="navbar-jsonViewer"
      >
        <div className={classes.toolbar} />
        <List component="nav" dense={true} className={classes.mainMenu}>
          <IconButton onClick={() => setIsJSONViewOpen(true)}>
            <CodeIcon />
          </IconButton>
          <Divider />
          <IconButton onClick={downloadPluginJSON}>
            <GetAppIcon />
          </IconButton>
        </List>
      </Drawer>
    </div>
  );
};

const StyledJsonLiveViewerView = withStyles(styles)(JsonLiveViewerView);
const JsonLiveViewer = createContextConnect(StyledJsonLiveViewerView);
export default JsonLiveViewer;
