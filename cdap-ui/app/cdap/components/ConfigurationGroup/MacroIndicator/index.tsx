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

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import IconButton from '@material-ui/core/IconButton';
import RadioButtonUnchecked from '@material-ui/icons/RadioButtonUnchecked';
import Lens from '@material-ui/icons/Lens';
import classnames from 'classnames';

const styles = (theme): StyleRules => {
  return {
    macroIndicator: {
      marginLeft: '10px',
      '&:focus': {
        outline: 0,
      },
    },
    macroIconContainer: {
      position: 'relative',
      display: 'flex', // this is to remove the extra space in angular apps between inline elements
    },
    macroM: {
      position: 'absolute',
      top: '50%',
      left: '50%',
      transform: 'translate(-50%, -50%)',
    },
    macroActive: {
      '& $macroM': {
        color: theme.palette.white[50],
      },
    },
  };
};

interface IMacroIndicatorProps extends WithStyles<typeof styles> {
  onClick: () => void;
  disabled: boolean;
  isActive: boolean;
}

const MacroIndicatorView: React.FC<IMacroIndicatorProps> = ({
  onClick,
  disabled,
  isActive,
  classes,
}) => {
  return (
    <IconButton
      size="small"
      color="primary"
      onClick={onClick}
      className={classes.macroIndicator}
      disabled={disabled}
      tabIndex={-1}
    >
      <span className={classnames(classes.macroIconContainer, { [classes.macroActive]: isActive })}>
        {isActive ? <Lens fontSize="large" /> : <RadioButtonUnchecked fontSize="large" />}
        <strong className={classes.macroM}>M</strong>
      </span>
    </IconButton>
  );
};

const MacroIndicator = withStyles(styles)(MacroIndicatorView);
export default MacroIndicator;
