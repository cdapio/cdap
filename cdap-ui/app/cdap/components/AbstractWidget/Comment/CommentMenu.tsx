/*
 * Copyright © 2021 Cask Data, Inc.
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
import Paper from '@material-ui/core/Paper';
import Popper from '@material-ui/core/Popper';
import MenuItem from '@material-ui/core/MenuItem';
import MenuList from '@material-ui/core/MenuList';
import MoreVertIcon from '@material-ui/icons/MoreVert';
import IconButton from '@material-ui/core/IconButton';
import makeStyle from '@material-ui/core/styles/makeStyles';
import { Theme } from '@material-ui/core/styles/createMuiTheme';
import ClickAwayListener from '@material-ui/core/ClickAwayListener';
import { preventPropagation, defaultEventObject } from 'services/helpers';
const useStyles = makeStyle<Theme>(() => {
  return {
    root: {
      position: 'relative',
    },
    popperRoot: {
      zIndex: 2000,
    },
  };
});

interface ICommentMenuProps {
  onEdit: () => void;
  onDelete: () => void;
}

export default function CommentMenu({ onEdit, onDelete }: ICommentMenuProps) {
  const classes = useStyles();
  const [anchorEl, setAnchorEl] = React.useState(null);
  const [isSettingsOpen, setIsSettingsOpen] = React.useState(false);
  const toggleSettingsMenu = (e = { ...defaultEventObject, currentTarget: null }) => {
    setAnchorEl(e.currentTarget);
    setIsSettingsOpen(!isSettingsOpen);
    preventPropagation(e);
  };
  return (
    <ClickAwayListener onClickAway={() => setIsSettingsOpen(false)}>
      <div className={classes.root}>
        <IconButton onClick={toggleSettingsMenu}>
          <MoreVertIcon />
        </IconButton>
        <Popper
          className={classes.popperRoot}
          anchorEl={anchorEl}
          open={isSettingsOpen}
          placement="bottom-end"
          disablePortal={false}
          modifiers={{
            flip: {
              enabled: false,
            },
            preventOverflow: {
              enabled: true,
              boundariesElement: 'window',
            },
            arrow: {
              enabled: false,
            },
          }}
        >
          <Paper>
            <MenuList autoFocusItem={isSettingsOpen} id="menu-list-grow">
              <MenuItem
                onClick={() => {
                  onEdit();
                  toggleSettingsMenu();
                }}
              >
                Edit
              </MenuItem>
              <MenuItem
                onClick={() => {
                  onDelete();
                  toggleSettingsMenu();
                }}
              >
                Delete
              </MenuItem>
            </MenuList>
          </Paper>
        </Popper>
      </div>
    </ClickAwayListener>
  );
}
