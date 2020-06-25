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

import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';
import { useConfigurationGroupState } from 'components/PluginJSONCreator/Create';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    groupInput: {
      marginTop: '10px',
      marginBottom: '10px',
    },
    groupInputContainer: {
      position: 'relative',
      padding: '7px 10px 5px',
      margin: '25px',
    },
  };
};

export const GroupInfoInputView = ({ classes, groupID }) => {
  const { groupToInfo, setGroupToInfo } = useConfigurationGroupState();

  function onGroupLabelChange() {
    return (label) => {
      setGroupToInfo(groupToInfo.setIn([groupID, 'label'], label));
    };
  }

  function onGroupDescriptionChange() {
    return (description) => {
      setGroupToInfo(groupToInfo.setIn([groupID, 'description'], description));
    };
  }

  const group = groupToInfo.get(groupID);
  return React.useMemo(
    () => (
      <div className={classes.groupInputContainer}>
        <div className={classes.groupInput}>
          <PluginInput
            widgetType={'textbox'}
            value={group.get('label')}
            onChange={onGroupLabelChange()}
            label={'Label'}
            required={true}
          />
        </div>
        <div className={classes.groupInput}>
          <PluginInput
            widgetType={'textarea'}
            value={group.get('description')}
            onChange={onGroupDescriptionChange()}
            label={'Description'}
            required={false}
          />
        </div>
      </div>
    ),
    [group]
  );
};

const GroupInfoInput = withStyles(styles)(GroupInfoInputView);
export default GroupInfoInput;
