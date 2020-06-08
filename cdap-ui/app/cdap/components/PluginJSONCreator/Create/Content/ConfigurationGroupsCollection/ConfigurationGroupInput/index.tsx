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

import ExpansionPanel from '@material-ui/core/ExpansionPanel';
import ExpansionPanelActions from '@material-ui/core/ExpansionPanelActions';
import ExpansionPanelSummary from '@material-ui/core/ExpansionPanelSummary';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import Typography from '@material-ui/core/Typography';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import If from 'components/If';
import GroupActionButtons from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupsCollection/GroupActionButtons';
import GroupInfoInput from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupsCollection/GroupInfoInput';
import WidgetCollection from 'components/PluginJSONCreator/Create/Content/WidgetCollection';
import {
  CreateContext,
  createContextConnect,
  ICreateContext,
} from 'components/PluginJSONCreator/CreateContextConnect';
import React from 'react';
import { Draggable } from 'react-beautiful-dnd';

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

interface IConfigurationGroupInputProps extends ICreateContext, WithStyles<typeof styles> {
  id: string;
  index: number;
  groupID: string;
  configurationGroupExpanded: boolean;
  switchEditConfigurationGroup: () => void;
  addConfigurationGroup: () => void;
  deleteConfigurationGroup: () => void;
  reorderConfigurationGroups: (groupID: string, afterGroupID: string) => void;
}

const ConfigurationGroupInputView: React.FC<IConfigurationGroupInputProps> = ({
  classes,
  id,
  index,
  groupID,
  configurationGroupExpanded,
  switchEditConfigurationGroup,
  addConfigurationGroup,
  deleteConfigurationGroup,
  reorderConfigurationGroups,
  groupToInfo,
  groupToWidgets,
  widgetInfo,
  widgetToAttributes,
  setGroupToInfo,
  setGroupToWidgets,
  setWidgetInfo,
  setWidgetToAttributes,
}) => {
  const group = groupToInfo[groupID];
  return (
    <Draggable draggableId={id} index={index} type="TASK">
      {(provided) => (
        <div
          className={classes.eachGroup}
          ref={provided.innerRef}
          {...provided.draggableProps}
          {...provided.dragHandleProps}
        >
          <ExpansionPanel
            expanded={configurationGroupExpanded}
            onChange={switchEditConfigurationGroup}
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
                groupID={groupID}
                groupToInfo={groupToInfo}
                setGroupToInfo={setGroupToInfo}
              />
              <WidgetCollection
                groupID={groupID}
                groupToWidgets={groupToWidgets}
                setGroupToWidgets={setGroupToWidgets}
                widgetInfo={widgetInfo}
                setWidgetInfo={setWidgetInfo}
                widgetToAttributes={widgetToAttributes}
                setWidgetToAttributes={setWidgetToAttributes}
              />
            </ExpansionPanelActions>
          </ExpansionPanel>

          <GroupActionButtons
            onAddConfigurationGroup={addConfigurationGroup}
            onDeleteConfigurationGroup={deleteConfigurationGroup}
          />
          {provided.placeholder}
        </div>
      )}
    </Draggable>
  );
};

const StyledConfigurationGroupInput = withStyles(styles)(ConfigurationGroupInputView);
const ConfigurationGroupInput = createContextConnect(CreateContext, StyledConfigurationGroupInput);
export default ConfigurationGroupInput;
