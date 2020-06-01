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

import { withStyles } from '@material-ui/core';
import { StyleRules } from '@material-ui/core/styles';
import If from 'components/If';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import {
  CreateContext,
  createContextConnect,
} from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    groupInput: {
      '& > *': {
        marginTop: '10px',
        marginBottom: '10px',
      },
    },
    groupInputContainer: {
      position: 'relative',
      padding: '7px 10px 5px',
      margin: '25px',
    },
  };
};

export const GroupInfoInputView = ({ classes, groupID, groupToInfo, setGroupToInfo }) => {
  function onGroupLabelChange() {
    return (label) => {
      setGroupToInfo((prevObjs) => ({
        ...prevObjs,
        [groupID]: { ...prevObjs[groupID], label },
      }));
    };
  }

  function onGroupDescriptionChange() {
    return (description) => {
      setGroupToInfo((prevObjs) => ({
        ...prevObjs,
        [groupID]: { ...prevObjs[groupID], description },
      }));
    };
  }

  const group = groupToInfo ? groupToInfo[groupID] : null;

  return (
    <If condition={group}>
      <div className={classes.groupInputContainer} data-cy="widget-wrapper-container">
        <div className={classes.groupInput}>
          <PluginInput
            widgetType={'textbox'}
            value={group.label}
            setValue={onGroupLabelChange()}
            label={'Label'}
            required={true}
          />
          <PluginInput
            widgetType={'textarea'}
            value={group.description}
            setValue={onGroupDescriptionChange()}
            label={'Description'}
            required={false}
          />
        </div>
      </div>
    </If>
  );
};

const StyledGroupInfoInput = withStyles(styles)(GroupInfoInputView);
const GroupInfoInput = createContextConnect(CreateContext, StyledGroupInfoInput);
export default GroupInfoInput;
