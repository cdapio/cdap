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

import { Button } from '@material-ui/core';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import If from 'components/If';
import WidgetPanel from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetPanel';
import { ICreateContext, IWidgetInfo } from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';
import uuidV4 from 'uuid/v4';

const styles = (theme): StyleRules => {
  return {
    nestedWidgets: {
      border: `1px solid`,
      borderColor: theme.palette.grey[300],
      borderRadius: '6px',
      position: 'relative',
      padding: '7px 10px 5px',
      margin: '25px',
    },
    addWidgetLabel: {
      fontSize: '12px',
      position: 'absolute',
      top: '-10px',
      left: '15px',
      padding: '0 5px',
      backgroundColor: theme.palette.white[50],
    },
    required: {
      fontSize: '14px',
      marginLeft: '5px',
      lineHeight: '12px',
      verticalAlign: 'middle',
    },
    widgetContainer: {
      width: 'calc(100%-1000px)',
    },
    widgetDivider: {
      width: '100%',
    },
  };
};

const WidgetCollectionView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  groupID,
  widgetToInfo,
  setWidgetToInfo,
  groupToWidgets,
  setGroupToWidgets,
  widgetToAttributes,
  setWidgetToAttributes,
}) => {
  const [activeWidgets, setActiveWidgets] = React.useState(groupID ? groupToWidgets[groupID] : []);
  const [openWidgetIndex, setOpenWidgetIndex] = React.useState(null);

  React.useEffect(() => {
    if (groupID) {
      setActiveWidgets(groupToWidgets[groupID]);
    } else {
      setActiveWidgets([]);
    }
  }, [groupToWidgets]);

  function addWidgetToGroup(index: number) {
    return () => {
      const newWidgetID = 'Widget_' + uuidV4();

      // Add a new widget's ID at the specified index
      const newWidgets = [...activeWidgets];
      if (newWidgets.length === 0) {
        newWidgets.splice(0, 0, newWidgetID);
      } else {
        newWidgets.splice(index + 1, 0, newWidgetID);
      }
      setGroupToWidgets({
        ...groupToWidgets,
        [groupID]: newWidgets,
      });

      // Set the default empty properties of the widget
      setWidgetToInfo({
        ...widgetToInfo,
        [newWidgetID]: {
          widgetType: '',
          label: '',
          name: '',
        } as IWidgetInfo,
      });

      setWidgetToAttributes({
        ...widgetToAttributes,
        [newWidgetID]: {},
      });
    };
  }

  function deleteWidgetFromGroup(widgetIndex) {
    return () => {
      // Grab the widget ID to delete
      const widgetToDelete = activeWidgets[widgetIndex];

      const newWidgets = [...activeWidgets];
      newWidgets.splice(widgetIndex, 1);
      setGroupToWidgets({
        ...groupToWidgets,
        [groupID]: newWidgets,
      });

      const { [widgetToDelete]: info, ...restWidgetToInfo } = widgetToInfo;
      setWidgetToInfo(restWidgetToInfo);

      const { [widgetToDelete]: attributes, ...restWidgetToAttributes } = widgetToAttributes;
      setWidgetToAttributes(restWidgetToAttributes);
    };
  }

  function openWidgetAttributes(widgetIndex) {
    return () => {
      setOpenWidgetIndex(widgetIndex);
    };
  }

  function closeWidgetAttributes() {
    setOpenWidgetIndex(null);
  }

  return (
    <div className={classes.nestedWidgets} data-cy="widget-wrapper-container">
      <div className={`widget-wrapper-label ${classes.addWidgetLabel}`}>
        Add Widgets
        <span className={classes.required}>*</span>
      </div>
      <div className={classes.widgetContainer}>
        {activeWidgets.map((widgetID, widgetIndex) => {
          return (
            <If condition={widgetToInfo[widgetID]}>
              <WidgetPanel
                widgetID={widgetID}
                closeWidgetAttributes={closeWidgetAttributes}
                openWidgetAttributes={openWidgetAttributes(widgetIndex)}
                addWidgetToGroup={addWidgetToGroup(widgetIndex)}
                deleteWidgetFromGroup={deleteWidgetFromGroup(widgetIndex)}
                widgetToAttributes={widgetToAttributes}
                setWidgetToAttributes={setWidgetToAttributes}
                widgetToInfo={widgetToInfo}
                setWidgetToInfo={setWidgetToInfo}
                widgetIndex={widgetIndex}
                widgetAttributesOpen={openWidgetIndex === widgetIndex}
                activeWidgets={activeWidgets}
              />
            </If>
          );
        })}
        <If condition={Object.keys(activeWidgets).length === 0}>
          <Button variant="contained" color="primary" onClick={addWidgetToGroup(0)}>
            Add Properties
          </Button>
        </If>
      </div>
    </div>
  );
};

const WidgetCollection = withStyles(styles)(WidgetCollectionView);
export default React.memo(WidgetCollection);
