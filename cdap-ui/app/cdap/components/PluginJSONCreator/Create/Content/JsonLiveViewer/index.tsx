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
  IWidgetInfo,
  OutputSchemaType,
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
  groupToInfo,
  groupToWidgets,
  widgetToInfo,
  widgetToAttributes,
  outputSchemaType,
  schemaTypes,
  filters,
  filterToName,
  filterToCondition,
  filterToShowList,
  showToInfo,
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
        /*if (widgetAttributes) {
          // TODO remove hardcoding
          Object.entries(widgetAttributes).forEach(([attributeField, attributeValue]) => {
            const attributeType = WIDGET_TYPE_TO_ATTRIBUTES[widgetType][attributeField].type;
            const isMultipleInput = attributeType.includes('[]');

            if (!isMultipleInput) {
              widgetAttributes[attributeField] = attributeValue;
            } else {
              if (typeof attributeValue === 'string') {
                widgetAttributes[attributeField] = attributeValue.split(COMMON_DELIMITER);
              } else {
                const supportedTypes = processSupportedTypes(attributeType.split('|'));
                // keyvalue pair
                if (supportedTypes.has(SupportedType.ValueLabelPair)) {
                  widgetAttributes[attributeField].forEach((attribute) => {
                    attribute.value = attribute.key;
                    attribute.label = attribute.value;
                  });
                } else {
                  widgetAttributes[attributeField].forEach((attribute) => {
                    attribute.id = attribute.key;
                    attribute.label = attribute.value;
                  });
                }
              }
            }
          });
        }*/
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

    let outputs;
    if (outputSchemaType === OutputSchemaType.Explicit) {
      outputs = {
        name: 'TODO',
        'widget-type': 'schema',
        'widget-attributes': {
          'schema-default-type': 'TODO',
          'schema-types': schemaTypes,
        },
      };
    } else {
      outputs = {
        name: 'TODO',
        'widget-type': 'non-editable-schema-editor',
        'widget-attributes': {
          TODO: 'TODO',
        },
      };
    }

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
      outputs,
      ...(filtersData &&
        Object.keys(filtersData).length > 0 && {
          filters: filtersData,
        }),
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
          rows={50}
          value={JSON.stringify(JSONConfig, undefined, 4)}
        ></JsonEditorWidget>
      </List>
    </Drawer>
  );
};

const StyledJsonLiveViewerView = withStyles(styles)(JsonLiveViewerView);
const JsonLiveViewer = createContextConnect(StyledJsonLiveViewerView);
export default JsonLiveViewer;
