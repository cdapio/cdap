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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import MenuItem from '@material-ui/core/MenuItem';
import Menu from '@material-ui/core/Menu';
import KeyboardArrowDownIcon from '@material-ui/icons/KeyboardArrowDown';
import Divider from '@material-ui/core/Divider';
import Button from '@material-ui/core/Button';
import T from 'i18n-react';
import { FllContext, IContextState } from 'components/FieldLevelLineage/v2/Context/FllContext';

const PREFIX = 'features.FieldLevelLineage.v2.FllTable';
const styles = (theme): StyleRules => {
  return {
    root: {
      paddingLeft: '47px',
    },
    targetView: {
      padding: 0,
      color: theme.palette.blue[200],
      minWidth: '60px',
      textAlign: 'right',
      textTransform: 'none',
      fontSize: 'inherit',
    },
    menu: {
      border: `1px solid ${theme.palette.grey[200]}`,
      borderRadius: '1px',
      color: theme.palette.blue[200],
      '& .MuiListItem-root': {
        minHeight: 0,
      },
      '& .Mui-disabled': {
        color: theme.palette.grey[200],
      },
    },
  };
};

interface IFllMenuProps extends WithStyles<typeof styles> {
  hasIncomingOps?: boolean;
  hasOutgoingOps?: boolean;
}

function FllMenu({ hasIncomingOps, hasOutgoingOps, classes }: IFllMenuProps) {
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const { handleViewCauseImpact, toggleOperations } = useContext<IContextState>(FllContext);

  function handleViewClick(e: React.MouseEvent<HTMLButtonElement>) {
    setAnchorEl(e.currentTarget);
  }

  function handleCloseMenu() {
    setAnchorEl(null);
  }

  function handleShowOperations(direction: string) {
    toggleOperations(direction);
    handleCloseMenu();
  }

  return (
    <span className={classes.root}>
      <Button onClick={handleViewClick} className={classes.targetView} data-cy="fll-view-dropdown">
        {T.translate(`${PREFIX}.FllField.viewDropdown`)}
        <KeyboardArrowDownIcon />
      </Button>
      <Menu
        classes={{ paper: classes.menu }}
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={handleCloseMenu}
        getContentAnchorEl={null}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
        transformOrigin={{ vertical: 'top', horizontal: 'center' }}
        data-cy="fll-field-menu"
      >
        <MenuItem onClick={handleViewCauseImpact} data-cy="fll-cause-impact">
          {T.translate(`${PREFIX}.FllMenu.causeImpact`)}
        </MenuItem>
        <Divider variant="middle" />
        <MenuItem
          onClick={hasIncomingOps ? handleShowOperations.bind(this, 'incoming') : undefined}
          disabled={!hasIncomingOps}
          data-cy="fll-view-incoming"
        >
          {T.translate(`${PREFIX}.FllMenu.viewIncoming`)}
        </MenuItem>
        <MenuItem
          onClick={hasOutgoingOps ? handleShowOperations.bind(this, 'outgoing') : undefined}
          disabled={!hasOutgoingOps}
          data-cy="fll-view-outgoing"
        >
          {T.translate(`${PREFIX}.FllMenu.viewOutgoing`)}
        </MenuItem>
      </Menu>
    </span>
  );
}

const StyledFllMenu = withStyles(styles)(FllMenu);

export default StyledFllMenu;
