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
import ExpansionPanel from '@material-ui/core/ExpansionPanel';
import ExpansionPanelActions from '@material-ui/core/ExpansionPanelActions';
import ExpansionPanelSummary from '@material-ui/core/ExpansionPanelSummary';
import IconButton from '@material-ui/core/IconButton';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import Typography from '@material-ui/core/Typography';
import AddIcon from '@material-ui/icons/Add';
import DeleteIcon from '@material-ui/icons/Delete';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import Heading, { HeadingTypes } from 'components/Heading';
import If from 'components/If';
import {
  createContextConnect,
  IConfigurationGroup,
  ICreateContext,
  IWidgetInfo,
} from 'components/PluginJSONCreator/Create';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import * as React from 'react';
import { WidgetTypes } from 'components/PluginJSONCreator/constants';
import Divider from '@material-ui/core/Divider';

const styles = (theme): StyleRules => {
  return {
    root: {
      padding: '30px 40px',
    },
    content: {
      width: '50%',
      maxWidth: '1000px',
      minWidth: '600px',
    },
    groupContent: {
      display: 'block',
      padding: '0px 0',
      width: 'calc(100%)',
    },
    groupInput: {
      '& > *': {
        width: '80%',
        marginTop: '10px',
        marginBottom: '10px',
      },
    },
    actionButtons: {
      float: 'right',
    },
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
    eachWidgetInput: {
      marginTop: '15px',
      marginBottom: '15px',
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
    errorBorder: {
      border: '2px solid',
      borderColor: theme.palette.red[50],
    },
    noWrapper: {
      border: 0,
      padding: 0,
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
      width: 'calc(100%)',
    },
    widgetDivider: {
      width: '100%',
    },
    tooltipContainer: {
      position: 'absolute',
      right: '5px',
      top: '10px',
    },
    focus: {
      borderColor: theme.palette.blue[200],
      '& $label': {
        color: theme.palette.blue[100],
      },
    },
  };
};

const GroupBasicInfoInput = ({ classes, label, setLabel, description, setDescription }) => {
  return (
    <div className={classes.nestedWidgets} data-cy="widget-wrapper-container">
      <div className={`widget-wrapper-label ${classes.label}`}>
        Configure Group
        <span className={classes.required}>*</span>
      </div>
      <div className={classes.widgetContainer}>
        <div className={classes.groupInput}>
          <GroupLabelInput value={label} setValue={setLabel} label={'Label'} />
          <GroupDescriptionInput
            value={description}
            setValue={setDescription}
            label={'Description'}
          />
        </div>
      </div>
    </div>
  );
};

export const GroupLabelInput = ({ setValue, value, label, placeholder = '', required = true }) => {
  const widget = {
    label,
    name: label,
    'widget-type': 'textbox',
    'widget-attributes': {
      default: value,
      placeholder,
    },
  };

  const property = {
    required,
    name: label,
  };

  return (
    <WidgetWrapper
      widgetProperty={widget}
      pluginProperty={property}
      value={value}
      onChange={setValue}
    />
  );
};

const GroupDescriptionInput = ({ setValue, value, label }) => {
  const widget = {
    label,
    name: label,
    'widget-type': 'textarea',
    'widget-attributes': {
      placeholder: 'Write a ' + label,
    },
  };

  const property = {
    required: false,
    name: 'description',
  };

  return (
    <WidgetWrapper
      widgetProperty={widget}
      pluginProperty={property}
      value={value}
      onChange={setValue}
    />
  );
};

const PluginSelect = ({ setValue, value, label, options }) => {
  const widget = {
    label,
    name: label,
    'widget-type': 'select',
    'widget-attributes': {
      options,
      default: options[0],
    },
  };

  const property = {
    required: true,
    name: label,
  };

  return (
    <WidgetWrapper
      widgetProperty={widget}
      pluginProperty={property}
      value={value}
      onChange={setValue}
    />
  );
};

const ConfigurationGroupsInfoView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  configurationGroups,
  setConfigurationGroups,
  groupToWidgets,
  setGroupToWidgets,
  widgetsToInfo,
  setWidgetsToInfo,
}) => {
  const [localConfigurationGroups, setLocalConfigurationGroups] = React.useState(
    configurationGroups
  );
  const [activeGroupIndex, setActiveGroupIndex] = React.useState(null);
  const activeGroup = localConfigurationGroups ? localConfigurationGroups[activeGroupIndex] : null;

  const [localGroupLabel, setLocalGroupLabel] = React.useState('');
  const [localGroupDescription, setLocalGroupDescription] = React.useState('');

  const [localGroupToWidgets, setLocalGroupToWidgets] = React.useState(groupToWidgets);
  const [localWidgetToInfo, setlocalWidgetToInfo] = React.useState(widgetsToInfo);

  const [activeWidgets, setActiveWidgets] = React.useState([]);

  React.useEffect(() => {
    if (localConfigurationGroups[activeGroupIndex]) {
      setLocalGroupLabel(localConfigurationGroups[activeGroupIndex].label);
      setLocalGroupDescription(localConfigurationGroups[activeGroupIndex].description);
    } /* else {
      setLocalGroupLabel('');
      setLocalGroupDescription('');
    }*/
    if (activeGroup) {
      setActiveWidgets(localGroupToWidgets[activeGroup.id]);
    }
  }, [activeGroupIndex]);

  React.useEffect(() => {
    if (activeGroup) {
      setActiveWidgets(localGroupToWidgets[activeGroup.id]);
    } else {
      setActiveWidgets([]);
    }
  }, [localGroupToWidgets, activeGroupIndex]);

  /*React.useEffect(() => {
    if (activeGroup) {
      const groupLabel = activeGroup.label;

      setLocalGroupToWidgets({
        ...localGroupToWidgets,
        [groupLabel]: Object.keys(localWidgetToInfo),
      });
    }
  }, [localWidgetToInfo]);*/

  // TODO change
  const requiredFilledOut = true;

  function handleAddLocalGroup(index) {
    addLocalConfigurationGroup(index);
    const group = activeGroup;
  }

  function handleDeleteLocalGroup(index) {
    deleteLocalConfigurationGroup(index);
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
    setlocalWidgetToInfo({
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
    setlocalWidgetToInfo(rest);
  }

  function addLocalConfigurationGroup(index: number) {
    const newLocalGroups = [...localConfigurationGroups];
    /*if (newLocalGroups.length > 0) {
      changeLocalConfigurationGroup(index);
      const groups = localConfigurationGroups;
    }*/

    const newGroupID =
      'ConfigGroup_' +
      Math.random()
        .toString(36)
        .substr(2, 9);
    const newGroup = {
      id: newGroupID,
      label: newGroupID, // generate a unique label
      description: '',
    } as IConfigurationGroup;

    setLocalGroupToWidgets({ ...localGroupToWidgets, [newGroupID]: [] });

    if (newLocalGroups.length == 0) {
      newLocalGroups.splice(0, 0, newGroup);
    } else {
      newLocalGroups.splice(index + 1, 0, newGroup);
    }

    setLocalConfigurationGroups(newLocalGroups);

    if (newLocalGroups.length <= 1) {
      setActiveGroupIndex(0);
    } else {
      setActiveGroupIndex(index + 1);
    }

    console.log('configurationGroups', JSON.stringify(newLocalGroups));
  }

  function deleteLocalConfigurationGroup(index: number) {
    const newLocalGroups = [...localConfigurationGroups];
    newLocalGroups.splice(index, 1);
    setLocalConfigurationGroups(newLocalGroups);
  }

  function changeLocalConfigurationGroup(index: number) {
    const newLocalGroups = [...localConfigurationGroups];
    const modifiedGroup = {
      id: newLocalGroups[index].id,
      label: localGroupLabel,
      description: localGroupDescription,
    } as IConfigurationGroup;
    newLocalGroups[index] = modifiedGroup;
    setLocalConfigurationGroups(newLocalGroups);
    const groups = localConfigurationGroups;
  }

  const switchEditConfigurationGroup = (index) => (event, newExpanded) => {
    const oldActiveGroupIndex = activeGroupIndex;
    if (!(newExpanded && activeGroupIndex == null)) {
      changeLocalConfigurationGroup(oldActiveGroupIndex);
    }
    if (newExpanded) {
      setActiveGroupIndex(index);
    } else {
      setActiveGroupIndex(null);
    }
  };

  function saveAllResults() {
    setConfigurationGroups(localConfigurationGroups);
    setGroupToWidgets(localGroupToWidgets);
  }

  function addWidgetToGroup(groupID: string, index?: number) {
    const newWidgetID =
      'Widget_' +
      Math.random()
        .toString(36)
        .substr(2, 9);

    if (index === undefined) {
      setlocalWidgetToInfo({
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
      setlocalWidgetToInfo({
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
    }
  }

  const EachWidgetInput = React.memo(
    ({
      classes,
      widgetObject,
      onNameChange,
      onLabelChange,
      onWidgetTypeChange,
      onAddWidget,
      onDeleteWidget,
    }) => {
      console.log('Rerendered:', widgetObject.name);
      return (
        <div>
          <div className={classes.widgetInput}>
            <GroupLabelInput
              value={widgetObject.name}
              setValue={onNameChange}
              label={'Widget Name'}
              placeholder={'Name a Widget'}
            />
            <GroupLabelInput
              value={widgetObject.label}
              setValue={onLabelChange}
              label={'Widget Label'}
              placeholder={'Label a Widget'}
            />
            <PluginSelect
              label={'Widget Type'}
              options={WidgetTypes}
              value={widgetObject.widgetType}
              setValue={onWidgetTypeChange}
            />
          </div>

          <div>
            <IconButton onClick={onAddWidget} data-cy="add-row">
              <AddIcon fontSize="small" />
            </IconButton>
            <IconButton onClick={onDeleteWidget} color="secondary" data-cy="remove-row">
              <DeleteIcon fontSize="small" />
            </IconButton>
          </div>
        </div>
      );
    },
    (prevProps, nextProps) => {
      const result =
        prevProps.widgetObject.label === nextProps.widgetObject.label &&
        prevProps.widgetObject.name === nextProps.widgetObject.name &&
        prevProps.widgetObject.widgetType == nextProps.widgetObject.widgetType;
      return result;
    }
  );

  const WidgetInput = ({ groupID }) => {
    function handleNameChange(obj, id) {
      return (name) => {
        setlocalWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, name } }));
      };
    }

    function handleLabelChange(obj, id) {
      return (label) => {
        setlocalWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, label } }));
      };
    }

    function handleWidgetTypeChange(obj, id) {
      return (widgetType) => {
        setlocalWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, widgetType } }));
      };
    }

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
                  <EachWidgetInput
                    classes={classes}
                    widgetObject={localWidgetToInfo[widgetID]}
                    onNameChange={handleNameChange(localWidgetToInfo[widgetID], widgetID)}
                    onLabelChange={handleLabelChange(localWidgetToInfo[widgetID], widgetID)}
                    onWidgetTypeChange={handleWidgetTypeChange(
                      localWidgetToInfo[widgetID],
                      widgetID
                    )}
                    onAddWidget={() => handleAddWidget(widgetIndex)}
                    onDeleteWidget={() => handleDeleteWidget(widgetIndex)}
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

  return (
    <div className={classes.root}>
      <div className={classes.content}>
        <Heading type={HeadingTypes.h3} label="Configuration Groups" />
        <br />
        <If condition={localConfigurationGroups.length == 0}>
          <Button variant="contained" color="primary" onClick={() => handleAddLocalGroup(0)}>
            Add Configuration Group
          </Button>
        </If>

        {localConfigurationGroups.map((group, i) => {
          const configurationGroupExpanded = activeGroupIndex == i;
          return (
            <ExpansionPanel
              expanded={configurationGroupExpanded}
              onChange={switchEditConfigurationGroup(i)}
            >
              <ExpansionPanelSummary
                expandIcon={<ExpandMoreIcon />}
                aria-controls="panel1c-content"
                id="panel1c-header"
              >
                <If condition={!configurationGroupExpanded}>
                  <Typography className={classes.heading}>{group.label}</Typography>
                </If>
              </ExpansionPanelSummary>
              <ExpansionPanelActions className={classes.groupContent}>
                <GroupBasicInfoInput
                  classes={classes}
                  label={localGroupLabel}
                  setLabel={setLocalGroupLabel}
                  description={localGroupDescription}
                  setDescription={setLocalGroupDescription}
                />
                <WidgetInput groupID={group.id} />
                <div className={classes.actionButtons}>
                  <IconButton onClick={() => handleAddLocalGroup(i)} data-cy="add-row">
                    <AddIcon fontSize="small" />
                  </IconButton>
                  <IconButton
                    onClick={() => handleDeleteLocalGroup(i)}
                    color="secondary"
                    data-cy="remove-row"
                  >
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                </div>
              </ExpansionPanelActions>
            </ExpansionPanel>
          );
        })}
      </div>
      <StepButtons nextDisabled={!requiredFilledOut} onNext={saveAllResults} />
    </div>
  );
};

const StyledConfigurationGroupsInfoView = withStyles(styles)(ConfigurationGroupsInfoView);
const ConfigurationGroupsInfo = createContextConnect(StyledConfigurationGroupsInfoView);
export default ConfigurationGroupsInfo;
