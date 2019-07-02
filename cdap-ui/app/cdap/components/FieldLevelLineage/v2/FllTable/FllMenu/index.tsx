/*
 * Copyright © 2019 Cask Data, Inc.
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

import React, { useState, useContext } from 'react';
import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';
import MenuItem from '@material-ui/core/MenuItem';
import Menu from '@material-ui/core/Menu';
import IconButton from '@material-ui/core/IconButton';
import KeyboardArrowDownIcon from '@material-ui/icons/KeyboardArrowDown';
import Divider from '@material-ui/core/Divider';
import { FllContext, IContextState } from 'components/FieldLevelLineage/v2/Context/FllContext';

const styles = (theme): StyleRules => {
  return {
    viewDropdown: {
      padding: 0,
      color: theme.palette.blue[200],
    },
  };
};

function FllMenu({ classes }) {
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const { handleViewCauseImpact } = useContext<IContextState>(FllContext);

  function handleViewClick(e: React.MouseEvent<HTMLButtonElement>) {
    setAnchorEl(e.currentTarget);
  }

  function handleClose() {
    setAnchorEl(null);
  }

  return (
    <span>
      <IconButton onClick={handleViewClick} className={classes.viewDropdown}>
        <KeyboardArrowDownIcon />
      </IconButton>
      <Menu
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={handleClose}
        getContentAnchorEl={null}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
        transformOrigin={{ vertical: 'top', horizontal: 'center' }}
      >
        <MenuItem onClick={handleViewCauseImpact}>Cause and impact</MenuItem>
        <Divider variant="middle" />
        <MenuItem onClick={handleClose}>Outgoing operations</MenuItem>
        <MenuItem onClick={handleClose}>Incoming operations</MenuItem>
      </Menu>
    </span>
  );
}

const StyledFllMenu = withStyles(styles)(FllMenu);

export default StyledFllMenu;
