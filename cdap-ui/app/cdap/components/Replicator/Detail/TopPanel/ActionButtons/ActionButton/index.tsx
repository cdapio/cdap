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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import LoadingSVG from 'components/LoadingSVG';
import classnames from 'classnames';

const styles = (theme): StyleRules => {
  return {
    root: {
      height: '50px',
      width: '60px',
      textAlign: 'center',
      backgroundColor: theme.palette.white[50],
      borderRadius: '4px',
      border: `1px solid ${theme.palette.grey[400]}`,
      cursor: 'pointer',
      marginLeft: '15px',
      userSelect: 'none',
      '&:hover': {
        backgroundColor: theme.palette.grey[800],
      },
    },
    icon: {
      fontSize: '18px',
    },
    loading: {
      height: '24px',
    },
    disabled: {
      color: theme.palette.grey[300],
      cursor: 'not-allowed',
      '&:hover': {
        backgroundColor: theme.palette.white[50],
      },
      '& svg': {
        color: theme.palette.grey[300],
      },
    },
  };
};

interface IActionButtonProps extends WithStyles<typeof styles> {
  icon: React.ReactNode;
  text: string;
  loading?: boolean;
  disabled: boolean;
  onClick: () => void;
}

const ActionButtonView: React.FC<IActionButtonProps> = ({
  classes,
  icon,
  text,
  loading,
  disabled,
  onClick,
}) => {
  function handleClick() {
    if (loading || disabled) {
      return;
    }

    onClick();
  }

  const loadingIcon = (
    <span className={classes.loading}>
      <LoadingSVG />
    </span>
  );

  return (
    <div
      className={classnames(classes.root, { [classes.disabled]: loading || disabled })}
      onClick={handleClick}
    >
      <div className={classes.icon}>{loading ? loadingIcon : icon}</div>
      <div>{text}</div>
    </div>
  );
};

const ActionButton = withStyles(styles)(ActionButtonView);
export default ActionButton;
