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

import { Button, Divider } from '@material-ui/core';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import If from 'components/If';
import { ICreateContext, IWidgetInfo } from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';
import uuidV4 from 'uuid/v4';
import WidgetActionButtons from './WidgetActionButtons';
import WidgetInput from './WidgetInput';

const styles = (theme): StyleRules => {
  return {
    eachWidget: {
      display: 'grid',
      gridTemplateColumns: '5fr 1fr',
      marginLeft: 'auto',
      marginRight: 'auto',
    },
    widgetInputs: {
      '& > *': {
        marginTop: '20px',
        marginBottom: '20px',
      },
    },
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

interface IWidgetCollectionProps extends WithStyles<typeof styles>, ICreateContext {
  groupID: number;
}

const WidgetCollectionView: React.FC<IWidgetCollectionProps> = ({
  classes,
  groupID,
  widgetToInfo,
  groupToWidgets,
  setWidgetToInfo,
  setGroupToWidgets,
}) => {
  const [activeWidgets, setActiveWidgets] = React.useState(groupID ? groupToWidgets[groupID] : []);

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
    };
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
              <div className={classes.eachWidget}>
                <div className={classes.widgetInputs}>
                  <WidgetInput
                    widgetToInfo={widgetToInfo}
                    widgetID={widgetID}
                    setWidgetToInfo={setWidgetToInfo}
                  />
                </div>
                <WidgetActionButtons
                  onAddWidgetToGroup={addWidgetToGroup(widgetIndex)}
                  onDeleteWidgetFromGroup={deleteWidgetFromGroup(widgetIndex)}
                />
              </div>
              <If condition={activeWidgets && widgetIndex < activeWidgets.length - 1}>
                <Divider className={classes.widgetDivider} />
              </If>
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
export default WidgetCollection;
