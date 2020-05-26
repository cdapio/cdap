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

import { Dialog, DialogContent, DialogTitle, IconButton } from '@material-ui/core';
import Button from '@material-ui/core/Button';
import Divider from '@material-ui/core/Divider';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import AddIcon from '@material-ui/icons/Add';
import CloseIcon from '@material-ui/icons/Close';
import DeleteIcon from '@material-ui/icons/Delete';
import Heading, { HeadingTypes } from 'components/Heading';
import If from 'components/If';
import { WIDGET_TYPE_TO_ATTRIBUTES } from 'components/PluginJSONCreator/constants';
import { IWidgetInfo } from 'components/PluginJSONCreator/Create';
import WidgetAttributesCollection from 'components/PluginJSONCreator/Create/Content/WidgetAttributesCollection';
import WidgetInput from 'components/PluginJSONCreator/Create/Content/WidgetInput';
import * as React from 'react';
import uuidV4 from 'uuid/v4';

const styles = (theme): StyleRules => {
  return {
    attributeDialog: {
      minHeight: '80vh',
      maxHeight: '80vh',
    },
    dialogTitle: {
      display: 'flex',
      justifyContent: 'flex-end',
      padding: '0 !important',
    },
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
    widgetActionButtons: {
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
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
  configurationGroups,
  activeWidgets,
  activeGroupIndex,
  widgetToInfo,
  groupToWidgets,
  setWidgetToInfo,
  setGroupToWidgets,
  widgetToAttributes,
  setWidgetToAttributes,
}) => {
  function addWidgetToGroup(index?: number) {
    const newWidgetID = 'Widget_' + uuidV4();

    const groupID = activeGroupID;

    if (index === undefined) {
      setWidgetToInfo({
        ...widgetToInfo,
        [newWidgetID]: {
          widgetType: '',
          label: '',
          name: '',
        } as IWidgetInfo,
      });
      setGroupToWidgets({
        ...groupToWidgets,
        [groupID]: [...groupToWidgets[groupID], newWidgetID],
      });
      setWidgetToAttributes({
        ...widgetToAttributes,
        [newWidgetID]: {},
      });
    } else {
      const widgets =
        activeGroupID && groupToWidgets[activeGroupID] ? groupToWidgets[activeGroupID] : [];
      if (widgets.length == 0) {
        widgets.splice(0, 0, newWidgetID);
      } else {
        widgets.splice(index + 1, 0, newWidgetID);
      }
      setWidgetToInfo({
        ...widgetToInfo,
        [newWidgetID]: {
          widgetType: '',
          label: '',
          name: '',
        } as IWidgetInfo,
      });
      setGroupToWidgets({
        ...groupToWidgets,
        [groupID]: widgets,
      });
      setWidgetToAttributes({
        ...widgetToAttributes,
        [newWidgetID]: {},
      });
    }
  }

  function deleteWidgetFromGroup(widgetIndex) {
    const groupID = activeGroupID;

    const widgets = groupToWidgets[groupID];

    const widgetToDelete = widgets[widgetIndex];

    widgets.splice(widgetIndex, 1);

    setGroupToWidgets({
      ...groupToWidgets,
      [groupID]: widgets,
    });

    const { [widgetToDelete]: info, ...restWidgetToInfo } = widgetToInfo;
    // delete widgetToDelete
    setWidgetToInfo(restWidgetToInfo);

    const { [widgetToDelete]: attributes, ...restWidgetToAttributes } = widgetToAttributes;
    setWidgetToAttributes(restWidgetToAttributes);
  }

  function handleNameChange(obj, id) {
    return (name) => {
      setWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, name } }));
    };
  }

  function handleLabelChange(obj, id) {
    return (label) => {
      setWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, label } }));
    };
  }

  function handleWidgetTypeChange(obj, id) {
    return (widgetType) => {
      setWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, widgetType } }));

      setWidgetToAttributes({
        ...widgetToAttributes,
        [id]: Object.keys(WIDGET_TYPE_TO_ATTRIBUTES[widgetType]).reduce((acc, curr) => {
          acc[curr] = '';
          return acc;
        }, {}),
      });
    };
  }

  function handleWidgetCategoryChange(obj, id) {
    return (widgetCategory) => {
      setWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, widgetCategory } }));
    };
  }

  function handleDialogClose() {
    setOpenWidgetIndex(null);
  }

  const activeGroupID = configurationGroups ? configurationGroups[activeGroupIndex] : null;
  const [openWidgetIndex, setOpenWidgetIndex] = React.useState(null);

  return (
    <div className={classes.nestedWidgets} data-cy="widget-wrapper-container">
      <div className={`widget-wrapper-label ${classes.label}`}>
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
                    widgetObject={widgetToInfo[widgetID]}
                    onNameChange={handleNameChange(widgetToInfo[widgetID], widgetID)}
                    onLabelChange={handleLabelChange(widgetToInfo[widgetID], widgetID)}
                    onWidgetTypeChange={handleWidgetTypeChange(widgetToInfo[widgetID], widgetID)}
                    onWidgetCategoryChange={handleWidgetCategoryChange(
                      widgetToInfo[widgetID],
                      widgetID
                    )}
                  />
                </div>
                <div className={classes.widgetActionButtons}>
                  <IconButton onClick={() => addWidgetToGroup(widgetIndex)} data-cy="add-row">
                    <AddIcon fontSize="small" />
                  </IconButton>
                  <IconButton
                    onClick={() => deleteWidgetFromGroup(widgetIndex)}
                    color="secondary"
                    data-cy="remove-row"
                  >
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                </div>

                <Dialog
                  open={openWidgetIndex === widgetIndex}
                  onClose={handleDialogClose}
                  disableBackdropClick={true}
                  fullWidth={true}
                  maxWidth={'md'}
                  classes={{ paper: classes.attributeDialog }}
                >
                  <DialogTitle disableTypography className={classes.dialogTitle}>
                    <IconButton onClick={handleDialogClose}>
                      <CloseIcon />
                    </IconButton>
                  </DialogTitle>
                  <DialogContent>
                    <Heading type={HeadingTypes.h3} label={'Set Widget Property'} />
                    <br></br>
                    <WidgetAttributesCollection
                      widgetID={widgetID}
                      widgetObject={widgetToInfo[widgetID]}
                      onNameChange={handleNameChange(widgetToInfo[widgetID], widgetID)}
                      onLabelChange={handleLabelChange(widgetToInfo[widgetID], widgetID)}
                      onWidgetTypeChange={handleWidgetTypeChange(widgetToInfo[widgetID], widgetID)}
                      onWidgetCategoryChange={handleWidgetCategoryChange(
                        widgetToInfo[widgetID],
                        widgetID
                      )}
                      onAddWidget={() => addWidgetToGroup(widgetIndex)}
                      onDeleteWidget={() => deleteWidgetFromGroup(widgetIndex)}
                      widgetToInfo={widgetToInfo}
                      widgetToAttributes={widgetToAttributes}
                      setWidgetToAttributes={setWidgetToAttributes}
                    />
                  </DialogContent>
                </Dialog>
              </div>
              <Button
                variant="contained"
                color="primary"
                component="span"
                onClick={() => setOpenWidgetIndex(widgetIndex)}
              >
                Widget Properties
              </Button>
              <If condition={activeWidgets && widgetIndex < activeWidgets.length - 1}>
                <Divider className={classes.widgetDivider} />
              </If>
            </If>
          );
        })}

        <If condition={Object.keys(activeWidgets).length == 0}>
          <Button variant="contained" color="primary" onClick={() => addWidgetToGroup(0)}>
            Add Properties
          </Button>
        </If>
      </div>
    </div>
  );
};

const WidgetCollection = withStyles(styles)(WidgetCollectionView);
export default WidgetCollection;
