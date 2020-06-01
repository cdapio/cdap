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

import {
  ExpansionPanel,
  ExpansionPanelActions,
  ExpansionPanelSummary,
  Typography,
  withStyles,
} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import Heading, { HeadingTypes } from 'components/Heading';
import If from 'components/If';
import GroupActionButtons from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupsCollection/GroupActionButtons';
import GroupInfoInput from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupsCollection/GroupInfoInput';
import JsonMenu from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import WidgetCollection from 'components/PluginJSONCreator/Create/Content/WidgetCollection';
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
  pluginName,
  pluginType,
  displayName,
  emitAlerts,
  emitErrors,
  configurationGroups,
  setConfigurationGroups,
  groupToInfo,
  setGroupToInfo,
  groupToWidgets,
  setGroupToWidgets,
  widgetToInfo,
  setWidgetToInfo,
  jsonView,
  setJsonView,
}) => {
  const [activeGroupIndex, setActiveGroupIndex] = React.useState(null);
  const [localConfigurationGroups, setLocalConfigurationGroups] = React.useState(
    configurationGroups
  );
  const [localGroupToInfo, setLocalGroupToInfo] = React.useState(groupToInfo);
  const [localGroupToWidgets, setLocalGroupToWidgets] = React.useState(groupToWidgets);
  const [localWidgetToInfo, setLocalWidgetToInfo] = React.useState(widgetToInfo);

  function addConfigurationGroup(index: number) {
    return () => {
      const newGroupID = 'ConfigGroup_' + uuidV4();

      // Add a new group's ID at the specified index
      const newGroups = [...localConfigurationGroups];
      if (newGroups.length === 0) {
        newGroups.splice(0, 0, newGroupID);
      } else {
        newGroups.splice(index + 1, 0, newGroupID);
      }
      setLocalConfigurationGroups(newGroups);

      // Set the activeGroupIndex to the new group's index
      if (newGroups.length <= 1) {
        setActiveGroupIndex(0);
      } else {
        setActiveGroupIndex(index + 1);
      }

      // Set the mappings for the newly added group
      setLocalGroupToInfo({
        ...localGroupToInfo,
        [newGroupID]: {
          label: '',
          description: '',
        } as IConfigurationGroupInfo,
      });
      setLocalGroupToWidgets({ ...localGroupToWidgets, [newGroupID]: [] });
    };
  }

  function deleteConfigurationGroup(index: number) {
    return () => {
      setActiveGroupIndex(null);

      // Delete a group at the specified index
      const newGroups = [...localConfigurationGroups];
      const groupToDelete = newGroups[index];
      newGroups.splice(index, 1);
      setLocalConfigurationGroups(newGroups);

      // Delete the corresponding data of the group
      const { [groupToDelete]: info, ...restGroupToInfo } = localGroupToInfo;
      const { [groupToDelete]: widgets, ...restGroupToWidgets } = localGroupToWidgets;
      setLocalGroupToInfo(restGroupToInfo);
      setLocalGroupToWidgets(restGroupToWidgets);

      // Delete all the widget information that belong to the group
      const newWidgetToInfo = localWidgetToInfo;
      widgets.map((widget) => {
        delete newWidgetToInfo[widget];
      });
      setLocalWidgetToInfo(newWidgetToInfo);
    };
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
    setGroupToWidgets(localGroupToWidgets);
    setWidgetToInfo(localWidgetToInfo);
  }

  return (
    <div>
      <JsonMenu
        pluginName={pluginName}
        pluginType={pluginType}
        displayName={displayName}
        emitAlerts={emitAlerts}
        emitErrors={emitErrors}
        configurationGroups={localConfigurationGroups}
        groupToInfo={localGroupToInfo}
        groupToWidgets={localGroupToWidgets}
        widgetToInfo={localWidgetToInfo}
        jsonView={jsonView}
        setJsonView={setJsonView}
      />
      <Heading type={HeadingTypes.h3} label="Configuration Groups" />
      <br />
      <If condition={localConfigurationGroups.length === 0}>
        <Button variant="contained" color="primary" onClick={addConfigurationGroup(0)}>
          Add Configuration Group
        </Button>
      </If>

      {localConfigurationGroups.map((groupID, i) => {
        const configurationGroupExpanded = activeGroupIndex === i;
        const group = localGroupToInfo[groupID];
        return (
          <div className={classes.eachGroup}>
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
                  groupID={groupID}
                  groupToWidgets={localGroupToWidgets}
                  setGroupToWidgets={setLocalGroupToWidgets}
                  widgetToInfo={localWidgetToInfo}
                  setWidgetToInfo={setLocalWidgetToInfo}
                />
              </ExpansionPanelActions>
            </ExpansionPanel>

            <GroupActionButtons
              onAddConfigurationGroup={addConfigurationGroup(i)}
              onDeleteConfigurationGroup={deleteConfigurationGroup(i)}
            />
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
