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

import React, { useState } from 'react';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';
import InputAdornment from '@material-ui/core/InputAdornment';
import IconButton from '@material-ui/core/IconButton';
import Visibility from '@material-ui/icons/Visibility';
import VisibilityOff from '@material-ui/icons/VisibilityOff';
import { IWidgetProps } from 'components/AbstractWidget';
import { objectQuery } from 'services/helpers';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import InputBase from '@material-ui/core/InputBase';

const styles = (theme) => {
  return {
    input: {
      padding: 10,
    },
    button: {
      padding: 0,
      '&:focus': {
        outline: 'none',
      },
    },
  };
};

interface IPasswordWidgetProps {
  placeholder?: string;
}

interface IPasswordProps extends IWidgetProps<IPasswordWidgetProps>, WithStyles<typeof styles> {}

function Password({ value, onChange, widgetProps, disabled, classes }: IPasswordProps) {
  const [pwdVisibility, setPwdVisibility] = useState<boolean>(false);
  const onChangeHandler = (event: React.ChangeEvent<HTMLInputElement>) => {
    const v = event.target.value;
    if (typeof onChange === 'function') {
      onChange(v);
    }
  };

  const handleClickShowPassword = () => {
    setPwdVisibility(!pwdVisibility);
  };

  const placeholder = objectQuery(widgetProps, 'placeholder');

  return (
    <InputBase
      fullWidth
      type={pwdVisibility ? 'text' : 'password'}
      value={value}
      onChange={onChangeHandler}
      placeholder={placeholder}
      disabled={disabled}
      startAdornment={
        <InputAdornment position="start">
          <IconButton
            className={classes.button}
            aria-label="Toggle password visibility"
            onClick={handleClickShowPassword}
          >
            {pwdVisibility ? <Visibility /> : <VisibilityOff />}
          </IconButton>
        </InputAdornment>
      }
    />
  );
}
const StyledPassword = withStyles(styles)(Password);

export default function StyledPasswordWrapper(props) {
  return (
    <ThemeWrapper>
      <StyledPassword {...props} />
    </ThemeWrapper>
  );
}

(StyledPasswordWrapper as any).propTypes = WIDGET_PROPTYPES;
