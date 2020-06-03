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
import GroupInfoInput from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupsCollection/GroupInfoInput';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import {
  CreateContext,
  createContextConnect,
  IConfigurationGroupInfo,
  ICreateContext,
} from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';
import uuidV4 from 'uuid/v4';

const styles = (): StyleRules => {
  return {
    eachGroup: {
      display: 'grid',
      gridTemplateColumns: '5fr 1fr',
    },
    groupContent: {
      display: 'block',
      padding: '0px 0',
      width: 'calc(100%)',
    },
  };
};

const ConfigurationGroupsCollectionView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  configurationGroups,
  setConfigurationGroups,
  groupToInfo,
  setGroupToInfo,
}) => {
  const [localConfigurationGroups, setLocalConfigurationGroups] = React.useState(
    configurationGroups
  );
  const [activeGroupIndex, setActiveGroupIndex] = React.useState(null);
  const [localGroupToInfo, setLocalGroupToInfo] = React.useState(groupToInfo);

  function addConfigurationGroup(index: number) {
    const newGroupID = 'ConfigGroup_' + uuidV4();

    // add a new group's ID at the specified index
    const newGroups = [...localConfigurationGroups];
    if (newGroups.length === 0) {
      newGroups.splice(0, 0, newGroupID);
    } else {
      newGroups.splice(index + 1, 0, newGroupID);
    }
    setLocalConfigurationGroups(newGroups);

    // set the activeGroupIndex to the new group's index
    if (newGroups.length <= 1) {
      setActiveGroupIndex(0);
    } else {
      setActiveGroupIndex(index + 1);
    }

    // set the default label and description of the group
    setLocalGroupToInfo({
      ...localGroupToInfo,
      [newGroupID]: {
        label: '',
        description: '',
      } as IConfigurationGroupInfo,
    });
  }

  function deleteConfigurationGroup(index: number) {
    setActiveGroupIndex(null);

    // delete a group at the specified index
    const newGroups = [...localConfigurationGroups];
    const groupToDelete = newGroups[index];
    newGroups.splice(index, 1);
    setLocalConfigurationGroups(newGroups);

    // delete the corresponding information of the group
    const { [groupToDelete]: info, ...restGroupToInfo } = localGroupToInfo;
    setLocalGroupToInfo(restGroupToInfo);
  }

  const switchEditConfigurationGroup = (index) => (event, newExpanded) => {
    if (newExpanded) {
      setActiveGroupIndex(index);
    } else {
      setActiveGroupIndex(null);
    }
  };

  function saveAllResults() {
    setConfigurationGroups(localConfigurationGroups);
    setGroupToInfo(localGroupToInfo);
  }

  return (
    <div>
      <Heading type={HeadingTypes.h3} label="Configuration Groups" />
      <br />
      <If condition={localConfigurationGroups.length === 0}>
        <Button variant="contained" color="primary" onClick={() => addConfigurationGroup(0)}>
          Add Configuration Group
        </Button>
      </If>

      {localConfigurationGroups.map((groupID, i) => {
        const configurationGroupExpanded = activeGroupIndex === i;
        const group = localGroupToInfo[groupID];
        return (
          <div key={groupID} className={classes.eachGroup}>
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
              </ExpansionPanelActions>
            </ExpansionPanel>

            <div className={classes.groupActionButtons}>
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
          </div>
        );
      })}
      <StepButtons nextDisabled={false} onPrevious={saveAllResults} onNext={saveAllResults} />
    </div>
  );
};

const StyledConfigurationGroupsCollectionView = withStyles(styles)(
  ConfigurationGroupsCollectionView
);
const ConfigurationGroupsCollection = createContextConnect(
  CreateContext,
  StyledConfigurationGroupsCollectionView
);
export default ConfigurationGroupsCollection;
