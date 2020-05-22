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

import Button from '@material-ui/core/Button';
import Divider from '@material-ui/core/Divider';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import If from 'components/If';
import { IWidgetInfo } from 'components/PluginJSONCreator/Create';
import WidgetAttributesInput, {
  WidgetTypeToAttribues,
} from 'components/PluginJSONCreator/Create/Content/WidgetAttributesInput';
import WidgetInput from 'components/PluginJSONCreator/Create/Content/WidgetInput';
import * as React from 'react';
import uuidV4 from 'uuid/v4';

const styles = (theme): StyleRules => {
  return {
    eachWidget: {
      display: 'block',
      marginLeft: 'auto',
      marginRight: 'auto',
      marginTop: '10px',
      marginBottom: '10px',
      // padding: '7px 10px 5px',
      '& > *': {
        //  margin: '6px',
        marginTop: '15px',
        marginBottom: '15px',
      },
    },
    widgetInput: {
      '& > *': {
        width: '80%',
        marginTop: '10px',
        marginBottom: '10px',
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
    label: {
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

const WidgetCollectionView: React.FC<WithStyles<typeof styles>> = ({
  classes,
  localConfigurationGroups,
  activeWidgets,
  activeGroupIndex,
  localWidgetToInfo,
  localGroupToWidgets,
  setLocalWidgetToInfo,
  setLocalGroupToWidgets,
  localWidgetToAttributes,
  setLocalWidgetToAttributes,
}) => {
  function addWidgetToGroup(groupID: string, index?: number) {
    const newWidgetID = 'Widget_' + uuidV4();

    if (index === undefined) {
      setLocalWidgetToInfo({
        ...localWidgetToInfo,
        [newWidgetID]: {
          widgetType: '',
          label: '',
          name: '',
        } as IWidgetInfo,
      });
      setLocalGroupToWidgets({
        ...localGroupToWidgets,
        [groupID]: [...localGroupToWidgets[groupID], newWidgetID],
      });
      setLocalWidgetToAttributes({
        ...localWidgetToAttributes,
        [newWidgetID]: {},
      });
    } else {
      const widgets =
        activeGroup.id && localGroupToWidgets[activeGroup.id]
          ? localGroupToWidgets[activeGroup.id]
          : [];
      if (widgets.length == 0) {
        widgets.splice(0, 0, newWidgetID);
      } else {
        widgets.splice(index + 1, 0, newWidgetID);
      }
      setLocalWidgetToInfo({
        ...localWidgetToInfo,
        [newWidgetID]: {
          widgetType: '',
          label: '',
          name: '',
        } as IWidgetInfo,
      });
      setLocalGroupToWidgets({
        ...localGroupToWidgets,
        [groupID]: widgets,
      });
      setLocalWidgetToAttributes({
        ...localWidgetToAttributes,
        [newWidgetID]: {},
      });
    }
  }

  function handleAddWidget(widgetIndex) {
    // add empty widget
    // 1. add widget into localGroupToWidgets
    /*const widgets =
      activeGroup.label && localGroupToWidgets[activeGroup.label]
        ? localGroupToWidgets[activeGroup.label]
        : [];
    if (widgets.length == 0) {
      widgets.splice(0, 0, '');
    } else {
      widgets.splice(widgetIndex + 1, 0, '');
    }
    setLocalGroupToWidgets({ ...localGroupToWidgets, [activeGroup.label]: widgets });
    // 2. add widget into localWidgetToInfo
    setLocalWidgetToInfo({
      ...localWidgetToInfo,
      [newWidget]: {
        widgetType: '',
        label: 'hi',
        name: 'hello',
      } as IWidgetInfo,
    });*/
    addWidgetToGroup(activeGroup.id, widgetIndex);
  }

  function handleDeleteWidget(widgetIndex) {
    // 1. delete widget from localGroupToWidgets
    // 2. delete widget key from localWidgetToInfo
    /*const newLocalGroups = [...localConfigurationGroups];
    newLocalGroups.splice(index, 1);
    setLocalConfigurationGroups(newLocalGroups);*/

    const groupID = activeGroup.id;

    const widgets = localGroupToWidgets[groupID];

    const widgetToDelete = widgets[widgetIndex];

    widgets.splice(widgetIndex, 1);

    setLocalGroupToWidgets({
      ...localGroupToWidgets,
      [groupID]: widgets,
    });

    const { [widgetToDelete]: tmp, ...rest } = localWidgetToInfo;
    // delete widgetToDelete
    setLocalWidgetToInfo(rest);

    const { [widgetToDelete]: tmp2, ...rest2 } = localWidgetToAttributes;
    setLocalWidgetToAttributes(rest2);
  }

  function handleNameChange(obj, id) {
    return (name) => {
      setLocalWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, name } }));
    };
  }

  function handleLabelChange(obj, id) {
    return (label) => {
      setLocalWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, label } }));
    };
  }

  function handleWidgetTypeChange(obj, id) {
    return (widgetType) => {
      setLocalWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, widgetType } }));

      if (widgetType) {
        setLocalWidgetToAttributes({
          ...localWidgetToAttributes,
          [id]: WidgetTypeToAttribues[widgetType],
        });
      }
    };
  }

  const activeGroup = localConfigurationGroups ? localConfigurationGroups[activeGroupIndex] : null;

  return (
    <div className={classes.nestedWidgets} data-cy="widget-wrapper-container">
      <If condition={true}>
        <div className={`widget-wrapper-label ${classes.label}`}>
          Add Widgets
          <span className={classes.required}>*</span>
        </div>
      </If>
      <div className={classes.widgetContainer}>
        {activeWidgets.map((widgetID, widgetIndex) => {
          return (
            <div>
              <div className={classes.eachWidget}>
                <WidgetInput
                  widgetObject={localWidgetToInfo[widgetID]}
                  onNameChange={handleNameChange(localWidgetToInfo[widgetID], widgetID)}
                  onLabelChange={handleLabelChange(localWidgetToInfo[widgetID], widgetID)}
                  onWidgetTypeChange={handleWidgetTypeChange(localWidgetToInfo[widgetID], widgetID)}
                  onAddWidget={() => handleAddWidget(widgetIndex)}
                  onDeleteWidget={() => handleDeleteWidget(widgetIndex)}
                />
                <WidgetAttributesInput
                  widgetID={widgetID}
                  widgetToInfo={localWidgetToInfo}
                  widgetToAttributes={localWidgetToAttributes}
                  setWidgetToAttributes={setLocalWidgetToAttributes}
                />
              </div>
              <Divider className={classes.widgetDivider} />
            </div>
          );
        })}

        <If condition={Object.keys(activeWidgets).length == 0}>
          <Button variant="contained" color="primary" onClick={() => handleAddWidget(0)}>
            Add Properties
          </Button>
        </If>
      </div>
    </div>
  );
};

const WidgetCollection = withStyles(styles)(WidgetCollectionView);
export default WidgetCollection;
