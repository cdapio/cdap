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

import Drawer from '@material-ui/core/Drawer';
import List from '@material-ui/core/List';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import JsonEditorWidget from 'components/AbstractWidget/CodeEditorWidget/JsonEditorWidget';
import { INamespaceLinkContext } from 'components/AppHeader/NamespaceLinkContext';
import {
  createContextConnect,
  IConfigurationGroup,
  IWidgetInfo,
} from 'components/PluginJSONCreator/Create';
import * as React from 'react';

const DRAWER_WIDTH = '600px';
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
    drawer: {
      zIndex: theme.zIndex.drawer,
      width: DRAWER_WIDTH,
    },
    drawerPaper: {
      width: DRAWER_WIDTH,
      backgroundColor: theme.palette.grey['700'],
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
  groupToWidgets,
  widgetToInfo,
  widgetToAttributes,
  open,
  onClose,
}) => {
  function getJSONConfig() {
    const configurationGroupsData = configurationGroups.map((group: IConfigurationGroup) => {
      const widgetData = groupToWidgets[group.id].map((widgetID: string) => {
        const widgetInfo: IWidgetInfo = widgetToInfo[widgetID];
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
        label: group.label,
        properties: widgetData,
      };
    });

    const config = {
      metadata: {
        'spec-version': 'TODO',
      },
      'display-name': displayName,
      'configuration-groups': configurationGroupsData,
      outputs: '[TODO]',
      'jump-config': '{TODO}',
    };

    return config;
  }

  const JSONConfig = getJSONConfig();

  return (
    <Drawer
      open={true}
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
      data-cy="navbar-drawer"
    >
      <div className={classes.toolbar} />
      <List component="nav" dense={true} className={classes.mainMenu}>
        <JsonEditorWidget
          rows={100}
          value={JSON.stringify(JSONConfig, undefined, 4)}
        ></JsonEditorWidget>
      </List>
      <List component="nav" dense={true} className={classes.namespaceAdminMenu}>
        hi
      </List>
    </Drawer>
  );
};

const StyledJsonLiveViewerView = withStyles(styles)(JsonLiveViewerView);
const JsonLiveViewer = createContextConnect(StyledJsonLiveViewerView);
export default JsonLiveViewer;
