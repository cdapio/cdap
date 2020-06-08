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

// using some little inline style helpers to make the app look okay(보기좋게 앱을 만드는 인라인 스타일 헬퍼)
const grid = 8;
const getItemStyle = (draggableStyle, isDragging) => ({
  // some basic styles to make the items look a bit nicer(아이템을 보기 좋게 만드는 몇 가지 기본 스타일)
  userSelect: 'none',
  padding: grid * 2,
  marginBottom: grid,

  // change background colour if dragging(드래깅시 배경색 변경)
  background: isDragging ? 'lightgreen' : 'white',

  // styles we need to apply on draggables(드래그에 필요한 스타일 적용)
  ...draggableStyle,
});
const getListStyle = (isDraggingOver) => ({
  background: isDraggingOver ? 'lightblue' : 'white',
  padding: grid,
});

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

  const reorderConfigurationGroups = (sourceIndex: number, destIndex: number) => {
    const newGroups = [...localConfigurationGroups];

    const sourceGroup = newGroups[sourceIndex];
    const destGroup = newGroups[destIndex];

    newGroups[sourceIndex] = destGroup;
    newGroups[destIndex] = sourceGroup;

    setLocalConfigurationGroups(newGroups);
  };

  const reorderWidgets = (
    sourceGroupID: string,
    destGroupID: string,
    sourceIndex: number,
    destIndex: number
  ) => {
    debugger;
    const sourceWidgets = [...localGroupToWidgets[sourceGroupID]];
    const destWidgets = [...localGroupToWidgets[destGroupID]];
    debugger;

    const sourceWidget = sourceWidgets[sourceIndex];
    const destWidget = destWidgets[destIndex];
    // If widgets are reordered inside the same configuration group
    if (sourceGroupID === destGroupID) {
      debugger;
      sourceWidgets[sourceIndex] = destWidget;
      sourceWidgets[destIndex] = sourceWidget;
      debugger;
      setLocalGroupToWidgets({ ...localGroupToWidgets, [sourceGroupID]: sourceWidgets });
    }
    // If widgets have been moved to a different configuration group
    else {
      const newSourceWidgets = [...sourceWidgets];
      const [draggedItem] = newSourceWidgets.splice(sourceIndex, 1);
      debugger;

      const newDestWidgets = [...destWidgets];
      newDestWidgets.splice(destIndex, 0, draggedItem);
      debugger;

      setLocalGroupToWidgets({
        ...localGroupToWidgets,
        [sourceGroupID]: newSourceWidgets,
        [destGroupID]: newDestWidgets,
      });
    }
  };

  function saveAllResults() {
    setConfigurationGroups(localConfigurationGroups);
    setGroupToInfo(localGroupToInfo);
    setGroupToWidgets(localGroupToWidgets);
    setWidgetInfo(localWidgetInfo);
    setWidgetToAttributes(localWidgetToAttributes);
  }

  const onDragEnd = (result) => {
    if (!result.destination) {
      return;
    }

    const sourceIndex = result.source.index;
    const destIndex = result.destination.index;

    if (result.type === 'groupItem') {
      reorderConfigurationGroups(sourceIndex, destIndex);
    } else if (result.type === 'widgetItem') {
      const sourceGroupID = result.source.droppableId;
      const destGroupID = result.destination.droppableId;

      reorderWidgets(sourceGroupID, destGroupID, sourceIndex, destIndex);
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
            <div ref={provided.innerRef} style={getListStyle(snapshot.isDraggingOver)}>
              {localConfigurationGroups.map((groupID, i) => {
                return (
                  <ConfigurationGroupInput
                    id={groupID}
                    key={groupID}
                    index={i}
                    groupID={groupID}
                    configurationGroupExpanded={activeGroupIndex === i}
                    switchEditConfigurationGroup={switchEditConfigurationGroup(i)}
                    addConfigurationGroup={addConfigurationGroup(i)}
                    deleteConfigurationGroup={deleteConfigurationGroup(i)}
                    reorderConfigurationGroups={reorderConfigurationGroups}
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
