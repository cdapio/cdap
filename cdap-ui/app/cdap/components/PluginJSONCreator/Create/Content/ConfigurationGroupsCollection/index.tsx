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
import Heading, { HeadingTypes } from 'components/Heading';
import If from 'components/If';
import ConfigurationGroupInput from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupsCollection/ConfigurationGroupInput';
import JsonMenu from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import {
  CreateContext,
  createContextConnect,
  IConfigurationGroupInfo,
  ICreateContext,
} from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';
import { DragDropContext, Droppable } from 'react-beautiful-dnd';
import uuidV4 from 'uuid/v4';

const ConfigurationGroupsCollectionView: React.FC<ICreateContext> = ({
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
  widgetInfo,
  setWidgetInfo,
  widgetToAttributes,
  setWidgetToAttributes,
  liveView,
  setLiveView,
  outputName,
  setPluginState,
  JSONStatus,
  setJSONStatus,
}) => {
  const [activeGroupIndex, setActiveGroupIndex] = React.useState(null);
  const [localConfigurationGroups, setLocalConfigurationGroups] = React.useState(
    configurationGroups
  );
  const [localGroupToInfo, setLocalGroupToInfo] = React.useState(groupToInfo);
  const [localGroupToWidgets, setLocalGroupToWidgets] = React.useState(groupToWidgets);
  const [localWidgetInfo, setLocalWidgetInfo] = React.useState(widgetInfo);
  const [localWidgetToAttributes, setLocalWidgetToAttributes] = React.useState(widgetToAttributes);

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
      const newWidgetInfo = localWidgetInfo;
      const newWidgetToAttributes = localWidgetToAttributes;
      widgets.map((widget) => {
        delete newWidgetInfo[widget];
        delete newWidgetToAttributes[widget];
      });
      setLocalWidgetInfo(newWidgetInfo);
      setLocalWidgetToAttributes(newWidgetToAttributes);
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
    setWidgetInfo(localWidgetInfo);
    setWidgetToAttributes(localWidgetToAttributes);
  }

  const reorderConfigurationGroups = (sourceIndex: number, destIndex: number) => {
    const newGroups = [...localConfigurationGroups];
    const [group] = newGroups.splice(sourceIndex, 1);
    newGroups.splice(destIndex, 0, group);

    setLocalConfigurationGroups(newGroups);
  };

  const reorderWidgets = (sourceWidgetIndex: number, destWidgetIndex: number) => {
    const activeGroupID = localConfigurationGroups[activeGroupIndex];
    const newSourceWidgets = [...localGroupToWidgets[activeGroupID]];
    const [draggedWidget] = newSourceWidgets.splice(sourceWidgetIndex, 1);
    newSourceWidgets.splice(destWidgetIndex, 0, draggedWidget);
    setLocalGroupToWidgets({ ...localGroupToWidgets, [activeGroupID]: newSourceWidgets });
  };

  const onDragEnd = (result) => {
    if (!result.destination) {
      return;
    }

    if (result.type === 'groupItem') {
      const sourceGroupID = result.source.index;
      const destGroupID = result.destination.index;
      reorderConfigurationGroups(sourceGroupID, destGroupID);
      // Switch the index if there is any open configuration group.
      setActiveGroupIndex(destGroupID);
    } else if (result.type === 'widgetItem') {
      const sourceWidgetIndex = result.source.index;
      const targetWidgetIndex = result.destination.index;
      reorderWidgets(sourceWidgetIndex, targetWidgetIndex);
    }
  };

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
        widgetInfo={localWidgetInfo}
        widgetToAttributes={localWidgetToAttributes}
        liveView={liveView}
        setLiveView={setLiveView}
        outputName={outputName}
        setPluginState={setPluginState}
        JSONStatus={JSONStatus}
        setJSONStatus={setJSONStatus}
      />
      <Heading type={HeadingTypes.h3} label="Configuration Groups" />
      <br />
      <If condition={localConfigurationGroups.length === 0}>
        <Button variant="contained" color="primary" onClick={addConfigurationGroup(0)}>
          Add Configuration Group
        </Button>
      </If>

      <DragDropContext onDragEnd={onDragEnd}>
        <Droppable droppableId="droppable" type={`groupItem`}>
          {(provided, snapshot) => (
            <div ref={provided.innerRef}>
              {localConfigurationGroups.map((groupID, i) => {
                return (
                  <ConfigurationGroupInput
                    key={groupID}
                    index={i}
                    groupID={groupID}
                    configurationGroupExpanded={activeGroupIndex === i}
                    switchEditConfigurationGroup={switchEditConfigurationGroup(i)}
                    addConfigurationGroup={addConfigurationGroup(i)}
                    deleteConfigurationGroup={deleteConfigurationGroup(i)}
                    groupToInfo={localGroupToInfo}
                    groupToWidgets={localGroupToWidgets}
                    widgetInfo={localWidgetInfo}
                    widgetToAttributes={localWidgetToAttributes}
                    setGroupToInfo={setLocalGroupToInfo}
                    setGroupToWidgets={setLocalGroupToWidgets}
                    setWidgetInfo={setLocalWidgetInfo}
                    setWidgetToAttributes={setLocalWidgetToAttributes}
                  />
                );
              })}
              {provided.placeholder}
            </div>
          )}
        </Droppable>
      </DragDropContext>

      <StepButtons nextDisabled={false} onPrevious={saveAllResults} onNext={saveAllResults} />
    </div>
  );
};

const ConfigurationGroupsCollection = createContextConnect(
  CreateContext,
  ConfigurationGroupsCollectionView
);
export default ConfigurationGroupsCollection;
