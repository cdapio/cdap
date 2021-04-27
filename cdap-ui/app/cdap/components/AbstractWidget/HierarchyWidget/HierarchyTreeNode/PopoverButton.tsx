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
import { IconButton, makeStyles, Popover, withStyles } from '@material-ui/core';
import AddIcon from '@material-ui/icons/Add';

const useStyles = makeStyles((theme) => ({
  popoverContainer: {
    padding: theme.spacing(1),
    width: '160px',
  },
}));

const ButtonIcon = withStyles(() => {
  return {
    root: {
      fontSize: '13px',
      borderRadius: '10px',
      padding: '6.5px',
    },
  };
})(IconButton);

interface IPopoverButtonProps {
  setShowMultiSelect?: () => void;
  addNewRecordToParent?: () => void;
}

const PopoverButton = ({ setShowMultiSelect, addNewRecordToParent }: IPopoverButtonProps) => {
  const [anchorEl, setAnchorEl] = React.useState(null);

  function handleClick(event) {
    setAnchorEl(event.currentTarget);
  }

  function handleClose() {
    setAnchorEl(null);
  }
  const open = Boolean(anchorEl);
  const classes = useStyles({});

  return (
    <React.Fragment>
      <AddIcon onClick={handleClick} />
      <Popover
        open={open}
        anchorEl={anchorEl}
        onClose={handleClose}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'center',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'center',
        }}
        data-cy="add-popup"
      >
        <fieldset className={classes.popoverContainer}>
          <ButtonIcon
            onClick={() => {
              setShowMultiSelect();
              setAnchorEl(null);
            }}
            data-cy="add-child"
          >
            Add a child field
          </ButtonIcon>
          <ButtonIcon
            onClick={() => {
              addNewRecordToParent();
              setAnchorEl(null);
            }}
          >
            Add a new record
          </ButtonIcon>
        </fieldset>
      </Popover>
    </React.Fragment>
  );
};

export default PopoverButton;
