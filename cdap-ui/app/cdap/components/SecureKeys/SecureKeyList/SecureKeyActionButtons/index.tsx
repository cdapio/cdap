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

import IconButton from '@material-ui/core/IconButton';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import MoreVertIcon from '@material-ui/icons/MoreVert';
import React from 'react';
import { preventPropagation } from 'services/helpers';

const styles = (theme): StyleRules => {
  return {
    secureKeyActionButtons: {
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
    },
  };
};

interface ISecureKeyActionButtonsProps extends WithStyles<typeof styles> {
  keyIndex: number;
  setActiveKeyIndex: (index: number) => void;
  setEditMode: (mode: boolean) => void;
  setDeleteMode: (mode: boolean) => void;
}

const SecureKeyActionButtonsView: React.FC<ISecureKeyActionButtonsProps> = ({
  classes,
  keyIndex,
  setActiveKeyIndex,
  setEditMode,
  setDeleteMode,
}) => {
  // Anchor element that appears when menu is clicked
  const [menuEl, setMenuEl] = React.useState(null);

  const handleMenuClick = (event) => {
    preventPropagation(event);
    setMenuEl(event.currentTarget);
  };

  const handleMenuClose = () => {
    setMenuEl(null);
  };

  const onDetailsClick = (event, index) => {
    preventPropagation(event);
    setActiveKeyIndex(index);
    setEditMode(true);
    setMenuEl(null);
  };

  const onDeleteClick = (event, index) => {
    preventPropagation(event);
    setActiveKeyIndex(index);
    setDeleteMode(true);
    setMenuEl(null);
  };

  return (
    <div className={classes.secureKeyActionButtons}>
      <div>
        <IconButton onClick={handleMenuClick}>
          <MoreVertIcon />
        </IconButton>
        <Menu anchorEl={menuEl} keepMounted open={Boolean(menuEl)} onClose={handleMenuClose}>
          <MenuItem onClick={(e) => onDetailsClick(e, keyIndex)}>Details</MenuItem>
          <MenuItem onClick={(e) => onDeleteClick(e, keyIndex)}>Delete</MenuItem>
        </Menu>
      </div>
    </div>
  );
};

const SecureKeyActionButtons = withStyles(styles)(SecureKeyActionButtonsView);
export default SecureKeyActionButtons;
