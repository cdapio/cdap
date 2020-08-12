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

import * as React from 'react';

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import AddIcon from '@material-ui/icons/Add';
import DeleteIcon from '@material-ui/icons/Delete';
import IconButton from '@material-ui/core/IconButton';

const styles = (): StyleRules => {
  return {
    groupActionButtons: {
      marginTop: '10px',
    },
  };
};

interface IGroupActionButtonsProps extends WithStyles<typeof styles> {
  onAddConfigurationGroup: () => void;
  onDeleteConfigurationGroup: () => void;
}

const GroupActionButtonsView: React.FC<IGroupActionButtonsProps> = ({
  classes,
  onAddConfigurationGroup,
  onDeleteConfigurationGroup,
}) => {
  return (
    <div className={classes.groupActionButtons}>
      <IconButton onClick={onAddConfigurationGroup} data-cy="add-configuration-group-btn">
        <AddIcon fontSize="small" />
      </IconButton>
      <IconButton
        onClick={onDeleteConfigurationGroup}
        color="secondary"
        data-cy="delete-configuration-group-btn"
      >
        <DeleteIcon fontSize="small" />
      </IconButton>
    </div>
  );
};

const GroupActionButtons = withStyles(styles)(GroupActionButtonsView);
export default GroupActionButtons;
