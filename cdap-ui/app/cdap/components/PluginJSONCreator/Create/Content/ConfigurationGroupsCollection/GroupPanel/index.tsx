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
import { StyleRules } from '@material-ui/core/styles';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import If from 'components/If';
import GroupActionButtons from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupsCollection/GroupActionButtons';
import GroupInfoInput from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupsCollection/GroupInfoInput';
import WidgetCollection from 'components/PluginJSONCreator/Create/Content/WidgetCollection';
import {
  CreateContext,
  createContextConnect,
} from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

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

export const GroupPanelView = ({
  classes,
  groupID,
  configurationGroupExpanded,
  switchEditConfigurationGroup,
  groupToInfo,
  setGroupToInfo,
  groupToWidgets,
  setGroupToWidgets,
  widgetToInfo,
  setWidgetToInfo,
  widgetToAttributes,
  setWidgetToAttributes,
  addConfigurationGroup,
  deleteConfigurationGroup,
}) => {
  return React.useMemo(
    () => (
      <div className={classes.eachGroup}>
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
              <Typography className={classes.heading}>{groupToInfo[groupID].label}</Typography>
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
              widgetToInfo={widgetToInfo}
              setWidgetToInfo={setWidgetToInfo}
              widgetToAttributes={widgetToAttributes}
              setWidgetToAttributes={setWidgetToAttributes}
            />
          </ExpansionPanelActions>
        </ExpansionPanel>

        <GroupActionButtons
          onAddConfigurationGroup={addConfigurationGroup}
          onDeleteConfigurationGroup={deleteConfigurationGroup}
        />
      </div>
    ),
    [configurationGroupExpanded]
  );
};

const StyledGroupPanel = withStyles(styles)(GroupPanelView);
const GroupPanel = createContextConnect(CreateContext, StyledGroupPanel);
export default GroupPanel;
