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
import Heading, { HeadingTypes } from 'components/Heading';
import If from 'components/If';
import {
  createContextConnect,
  IConfigurationGroup,
  ICreateContext,
} from 'components/PluginJSONCreator/Create';
import JsonLiveViewer from 'components/PluginJSONCreator/Create/Content/JsonLiveViewer';
import PluginTextareaInput from 'components/PluginJSONCreator/Create/Content/PluginTextareaInput';
import PluginTextboxInput from 'components/PluginJSONCreator/Create/Content/PluginTextboxInput';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import WidgetCollection from 'components/PluginJSONCreator/Create/Content/WidgetCollection';
import * as React from 'react';
import uuidV4 from 'uuid/v4';

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
    groupInputContainer: {
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
    widgetContainer: {
      width: 'calc(100%-1000px)',
    },
  };
};

const GroupBasicInfoInput = ({ classes, label, setLabel, description, setDescription }) => {
  return (
    <div className={classes.groupInputContainer} data-cy="widget-wrapper-container">
      <div className={classes.widgetContainer}>
        <div className={classes.groupInput}>
          <PluginTextboxInput value={label} setValue={setLabel} label={'Label'} />
          <PluginTextareaInput
            value={description}
            setValue={setDescription}
            label={'Description'}
          />
        </div>
      </div>
    </div>
  );
};

const ConfigurationGroupsInfoView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  configurationGroups,
  setConfigurationGroups,
  groupToWidgets,
  setGroupToWidgets,
  widgetToInfo,
  setWidgetToInfo,
  displayName,
  widgetToAttributes,
  setWidgetToAttributes,
}) => {
  const [localConfigurationGroups, setLocalConfigurationGroups] = React.useState(
    configurationGroups
  );
  const [activeGroupIndex, setActiveGroupIndex] = React.useState(null);
  const activeGroup = localConfigurationGroups ? localConfigurationGroups[activeGroupIndex] : null;

  const [localGroupLabel, setLocalGroupLabel] = React.useState('');
  const [localGroupDescription, setLocalGroupDescription] = React.useState('');

  const [localGroupToWidgets, setLocalGroupToWidgets] = React.useState(groupToWidgets);
  const [localWidgetToInfo, setLocalWidgetToInfo] = React.useState(widgetToInfo);
  const [localWidgetToAttributes, setLocalWidgetToAttributes] = React.useState(widgetToAttributes);

  const [activeWidgets, setActiveWidgets] = React.useState([]);

  const [jsonView, setJsonView] = React.useState(false);

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

  function addConfigurationGroup(index: number) {
    const newLocalGroups = [...localConfigurationGroups];

    const newGroupID = 'ConfigGroup_' + uuidV4();
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

  function deleteConfigurationGroup(index: number) {
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

  return (
    <div className={classes.root}>
      <JsonLiveViewer
        displayName={displayName}
        configurationGroups={localConfigurationGroups}
        groupToWidgets={localGroupToWidgets}
        widgetToInfo={localWidgetToInfo}
        widgetToAttributes={localWidgetToAttributes}
        open={jsonView}
        onClose={() => setJsonView(!jsonView)}
      />
      <div className={classes.content}>
        <Heading type={HeadingTypes.h3} label="Configuration Groups" />
        <br />
        <If condition={localConfigurationGroups.length == 0}>
          <Button variant="contained" color="primary" onClick={() => addConfigurationGroup(0)}>
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
                <WidgetCollection
                  activeWidgets={activeWidgets}
                  activeGroupIndex={activeGroupIndex}
                  setLocalGroupToWidgets={setLocalGroupToWidgets}
                  groupID={group.id}
                  localConfigurationGroups={localConfigurationGroups}
                  localGroupToWidgets={localGroupToWidgets}
                  localWidgetToInfo={localWidgetToInfo}
                  setLocalWidgetToInfo={setLocalWidgetToInfo}
                  localWidgetToAttributes={localWidgetToAttributes}
                  setLocalWidgetToAttributes={setLocalWidgetToAttributes}
                />
                <div className={classes.actionButtons}>
                  <IconButton onClick={() => addConfigurationGroup(i)} data-cy="add-row">
                    <AddIcon fontSize="small" />
                  </IconButton>
                  <IconButton
                    onClick={() => deleteConfigurationGroup(i)}
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
