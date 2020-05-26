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
  IConfigurationGroupInfo,
  ICreateContext,
} from 'components/PluginJSONCreator/Create';
import GroupInfoInput from 'components/PluginJSONCreator/Create/Content/GroupInfoInput';
import JsonLiveViewer from 'components/PluginJSONCreator/Create/Content/JsonLiveViewer';
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
    actionButtons: {},
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

const ConfigurationGroupsInfoView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  configurationGroups,
  setConfigurationGroups,
  groupToInfo,
  setGroupToInfo,
  groupToWidgets,
  setGroupToWidgets,
  widgetToInfo,
  setWidgetToInfo,
  displayName,
  widgetToAttributes,
  setWidgetToAttributes,
  outputSchemaType,
  schemaTypes,
  filters,
  filterToName,
  filterToCondition,
  filterToShowList,
  showToInfo,
}) => {
  console.log('configuration group rerendering');

  const [localConfigurationGroups, setLocalConfigurationGroups] = React.useState(
    configurationGroups
  );
  const [activeGroupIndex, setActiveGroupIndex] = React.useState(null);
  const activeGroupID = localConfigurationGroups
    ? localConfigurationGroups[activeGroupIndex]
    : null;

  const [localGroupLabel, setLocalGroupLabel] = React.useState('');
  const [localGroupDescription, setLocalGroupDescription] = React.useState('');

  const [localGroupToInfo, setLocalGroupToInfo] = React.useState(groupToInfo);
  const [localGroupToWidgets, setLocalGroupToWidgets] = React.useState(groupToWidgets);
  const [localWidgetToInfo, setLocalWidgetToInfo] = React.useState(widgetToInfo);
  const [localWidgetToAttributes, setLocalWidgetToAttributes] = React.useState(widgetToAttributes);

  const [activeWidgets, setActiveWidgets] = React.useState([]);

  const [jsonView, setJsonView] = React.useState(false);

  React.useEffect(() => {
    if (activeGroupID) {
      const activeGroupInfo = localGroupToInfo[activeGroupID];
      setLocalGroupLabel(activeGroupInfo.label);
      setLocalGroupDescription(activeGroupInfo.description);
    } /* else {
      setLocalGroupLabel('');
      setLocalGroupDescription('');
    }*/
    if (activeGroupID) {
      setActiveWidgets(localGroupToWidgets[activeGroupID]);
    }
  }, [activeGroupIndex]);

  React.useEffect(() => {
    if (activeGroupID) {
      setActiveWidgets(localGroupToWidgets[activeGroupID]);
    } else {
      setActiveWidgets([]);
    }
  }, [localGroupToWidgets, activeGroupIndex]);

  // TODO change
  const requiredFilledOut = true;

  function addConfigurationGroup(index: number) {
    const newGroupID = 'ConfigGroup_' + uuidV4();

    const newGroups = [...localConfigurationGroups];

    if (newGroups.length == 0) {
      newGroups.splice(0, 0, newGroupID);
    } else {
      newGroups.splice(index + 1, 0, newGroupID);
    }

    setLocalConfigurationGroups(newGroups);

    if (newGroups.length <= 1) {
      setActiveGroupIndex(0);
    } else {
      setActiveGroupIndex(index + 1);
    }

    setLocalGroupToInfo({
      ...localGroupToInfo,
      [newGroupID]: {
        label: newGroupID,
        description: '',
      } as IConfigurationGroupInfo,
    });
    setLocalGroupToWidgets({ ...localGroupToWidgets, [newGroupID]: [] });
  }

  function deleteConfigurationGroup(index: number) {
    setActiveGroupIndex(null);

    const newGroups = [...localConfigurationGroups];
    const groupToDelete = newGroups[index];
    newGroups.splice(index, 1);
    setLocalConfigurationGroups(newGroups);

    // groupToInfo
    const { [groupToDelete]: info, ...restGroupToInfo } = localGroupToInfo;
    setLocalGroupToInfo(restGroupToInfo);

    // groupToWidgets
    const { [groupToDelete]: widgets, ...restGroupToWidgets } = localGroupToWidgets;
    setLocalGroupToWidgets(restGroupToWidgets);

    // widgetToInfo
    // widgetToAttributes
    const newWidgetToInfo = localWidgetToInfo;
    const newWidgetToAttributes = localWidgetToAttributes;

    widgets.map((widget) => {
      delete newWidgetToInfo[widget];
      delete newWidgetToAttributes[widget];
    });

    setLocalWidgetToInfo(newWidgetToInfo);
    setLocalWidgetToAttributes(newWidgetToAttributes);
  }

  function modifyConfigurationGroup(index: number) {
    const newLocalGroups = [...localConfigurationGroups];
    /*const editGroupID = newLocalGroups[index];
    newLocalGroups[index] = editGroupID;
    setLocalConfigurationGroups(newLocalGroups);*/
    const editGroupID = newLocalGroups[index];
    const editGroup = {
      label: localGroupLabel,
      description: localGroupDescription,
    } as IConfigurationGroupInfo;
    const groups = localConfigurationGroups;
    setLocalGroupToInfo({ ...localGroupToInfo, [editGroupID]: editGroup });
  }

  const switchEditConfigurationGroup = (index) => (event, newExpanded) => {
    const oldActiveGroupIndex = activeGroupIndex;
    if (!(newExpanded && activeGroupIndex == null)) {
      modifyConfigurationGroup(oldActiveGroupIndex);
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
    setGroupToInfo(localGroupToInfo);
  }

  return (
    <div className={classes.root}>
      <JsonLiveViewer
        displayName={displayName}
        configurationGroups={localConfigurationGroups}
        groupToInfo={localGroupToInfo}
        groupToWidgets={localGroupToWidgets}
        widgetToInfo={localWidgetToInfo}
        widgetToAttributes={localWidgetToAttributes}
        outputSchemaType={outputSchemaType}
        schemaTypes={schemaTypes}
        open={jsonView}
        onClose={() => setJsonView(!jsonView)}
        filters={filters}
        filterToName={filterToName}
        filterToCondition={filterToCondition}
        filterToShowList={filterToShowList}
        showToInfo={showToInfo}
      />
      <div className={classes.content}>
        <Heading type={HeadingTypes.h3} label="Configuration Groups" />
        <br />
        <If condition={localConfigurationGroups.length == 0}>
          <Button variant="contained" color="primary" onClick={() => addConfigurationGroup(0)}>
            Add Configuration Group
          </Button>
        </If>

        {localConfigurationGroups.map((groupID, i) => {
          const configurationGroupExpanded = activeGroupIndex == i;
          const group = localGroupToInfo[groupID];
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
                <GroupInfoInput
                  classes={classes}
                  groupID={groupID}
                  groupToInfo={localGroupToInfo}
                  setGroupToInfo={setLocalGroupToInfo}
                />
                <WidgetCollection
                  activeWidgets={activeWidgets}
                  activeGroupIndex={activeGroupIndex}
                  setGroupToWidgets={setLocalGroupToWidgets}
                  groupID={groupID}
                  configurationGroups={localConfigurationGroups}
                  groupToWidgets={localGroupToWidgets}
                  widgetToInfo={localWidgetToInfo}
                  setWidgetToInfo={setLocalWidgetToInfo}
                  widgetToAttributes={localWidgetToAttributes}
                  setWidgetToAttributes={setLocalWidgetToAttributes}
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
