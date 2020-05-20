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

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import {
  createContextConnect,
  ICreateContext,
  IConfigurationGroup,
} from 'components/PluginJSONCreator/Create';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import Heading, { HeadingTypes } from 'components/Heading';
import Accordion, { AccordionContent, AccordionTitle, AccordionPane } from 'components/Accordion';
import Button from '@material-ui/core/Button';
import If from 'components/If';
import IconButton from '@material-ui/core/IconButton';
import AddIcon from '@material-ui/icons/Add';
import DeleteIcon from '@material-ui/icons/Delete';
import ExpansionPanel from '@material-ui/core/ExpansionPanel';
import ExpansionPanelDetails from '@material-ui/core/ExpansionPanelDetails';
import ExpansionPanelSummary from '@material-ui/core/ExpansionPanelSummary';
import ExpansionPanelActions from '@material-ui/core/ExpansionPanelActions';
import Divider from '@material-ui/core/Divider';
import Typography from '@material-ui/core/Typography';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';

const styles = (): StyleRules => {
  return {
    root: {
      padding: '30px 40px',
    },
    content: {
      width: '50%',
      maxWidth: '1000px',
      minWidth: '600px',
    },
    groupDetails: {
      width: '90%',
      textAlign: 'center',
    },
    groupInputs: {
      margin: 'auto',
    },
    actionButtons: {
      float: 'right',
    },
  };
};

const GroupBasicInfoInput = ({
  classes,
  label,
  setLabel,
  description,
  setDescription,
  handleAddGroup,
  handleDeleteGroup,
}) => {
  return (
    <div className={classes.groupDetails}>
      <div className={classes.groupInputs}>
        <GroupLabelInput value={label} setValue={setLabel} label={'Group Label'} />
        <br />
        <br />
        <GroupDescriptionInput
          value={description}
          setValue={setDescription}
          label={'Group Description'}
        />
      </div>

      <div className={classes.actionButtons}>
        <IconButton onClick={handleAddGroup} data-cy="add-row">
          <AddIcon fontSize="small" />
        </IconButton>
        <IconButton onClick={handleDeleteGroup} color="secondary" data-cy="remove-row">
          <DeleteIcon fontSize="small" />
        </IconButton>
      </div>
    </div>
  );
};

const GroupLabelInput = ({ setValue, value, label }) => {
  const widget = {
    label,
    name: label,
    'widget-type': 'textbox',
    'widget-attributes': {
      default: value,
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

const ConfigurationGroupsInfoView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  configurationGroups,
  setConfigurationGroups,
}) => {
  const [localConfigurationGroups, setLocalConfigurationGroups] = React.useState(
    configurationGroups
  );
  const [activeGroupIndex, setActiveGroupIndex] = React.useState(null);
  const activeGroup = localConfigurationGroups ? localConfigurationGroups[activeGroupIndex] : null;

  const [localGroupLabel, setLocalGroupLabel] = React.useState('');
  const [localGroupDescription, setLocalGroupDescription] = React.useState('');

  React.useEffect(() => {
    debugger;
    if (localConfigurationGroups[activeGroupIndex]) {
      setLocalGroupLabel(localConfigurationGroups[activeGroupIndex].label);
      setLocalGroupDescription(localConfigurationGroups[activeGroupIndex].description);
      debugger;
    } /* else {
      setLocalGroupLabel('');
      setLocalGroupDescription('');
      debugger;
    }*/
  }, [activeGroupIndex]);

  // TODO change
  const requiredFilledOut = true;

  function handleAddLocalGroup(index) {
    addLocalConfigurationGroup(index);
    const group = activeGroup;
    debugger;
  }

  function handleDeleteLocalGroup(index) {
    deleteLocalConfigurationGroup(index);
    debugger;
  }

  function addLocalConfigurationGroup(index: number) {
    const newLocalGroups = [...localConfigurationGroups];
    if (newLocalGroups.length > 0) {
      changeLocalConfigurationGroup(index);
      const groups = localConfigurationGroups;
      debugger;
    }

    const newGroup = {
      label:
        'ConfigGroup_' +
        Math.random()
          .toString(36)
          .substr(2, 9), // generate a unique label
      description: '',
    } as IConfigurationGroup;
    debugger;

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
    debugger;
  }

  function deleteLocalConfigurationGroup(index: number) {
    const newLocalGroups = [...localConfigurationGroups];
    newLocalGroups.splice(index, 1);
    setLocalConfigurationGroups(newLocalGroups);
  }

  function changeLocalConfigurationGroup(index: number) {
    const modifiedGroup = {
      label: localGroupLabel,
      description: localGroupDescription,
    } as IConfigurationGroup;
    const newLocalGroups = [...localConfigurationGroups];
    newLocalGroups[index] = modifiedGroup;
    setLocalConfigurationGroups(newLocalGroups);
    const groups = localConfigurationGroups;
    debugger;
  }

  const switchEditConfigurationGroup = (index) => (event, newExpanded) => {
    debugger;
    const oldActiveGroupIndex = activeGroupIndex;
    changeLocalConfigurationGroup(oldActiveGroupIndex);
    if (newExpanded) {
      setActiveGroupIndex(index);
      debugger;
    } else {
      setActiveGroupIndex(null);
      debugger;
    }
  };

  function saveConfigurationGroups() {
    setConfigurationGroups(localConfigurationGroups);
  }

  return (
    <div className={classes.root}>
      <div className={classes.content}>
        <Heading type={HeadingTypes.h3} label="Configuration Groups" />
        <br />
        <If condition={localConfigurationGroups.length == 0}>
          <IconButton onClick={() => handleAddLocalGroup(0)} data-cy="add-row">
            <AddIcon fontSize="small" />
          </IconButton>
        </If>

        {localConfigurationGroups.map((group, i) => {
          return (
            <ExpansionPanel
              expanded={activeGroupIndex == i}
              onChange={switchEditConfigurationGroup(i)}
            >
              <ExpansionPanelSummary
                expandIcon={<ExpandMoreIcon />}
                aria-controls="panel1c-content"
                id="panel1c-header"
              >
                <Typography className={classes.heading}>{group.label}</Typography>
              </ExpansionPanelSummary>
              <ExpansionPanelActions>
                <GroupBasicInfoInput
                  classes={classes}
                  label={localGroupLabel}
                  setLabel={setLocalGroupLabel}
                  description={localGroupDescription}
                  setDescription={setLocalGroupDescription}
                  handleAddGroup={() => handleAddLocalGroup(i)}
                  handleDeleteGroup={() => handleDeleteLocalGroup(i)}
                />
              </ExpansionPanelActions>
            </ExpansionPanel>
          );
        })}
      </div>
      <StepButtons nextDisabled={!requiredFilledOut} onNext={saveConfigurationGroups} />
    </div>
  );
};

const StyledConfigurationGroupsInfoView = withStyles(styles)(ConfigurationGroupsInfoView);
const ConfigurationGroupsInfo = createContextConnect(StyledConfigurationGroupsInfoView);
export default ConfigurationGroupsInfo;
