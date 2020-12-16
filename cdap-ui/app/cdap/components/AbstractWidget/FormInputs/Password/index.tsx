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

import IconButton from '@material-ui/core/IconButton';
import InputAdornment from '@material-ui/core/InputAdornment';
import InputBase from '@material-ui/core/InputBase';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import Visibility from '@material-ui/icons/Visibility';
import VisibilityOff from '@material-ui/icons/VisibilityOff';
import { IWidgetProps } from 'components/AbstractWidget';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import ThemeWrapper from 'components/ThemeWrapper';
import React, { useState } from 'react';
import { objectQuery } from 'services/helpers';

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

function Password({ value, onChange, widgetProps, disabled, classes, dataCy }: IPasswordProps) {
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
            tabIndex={-1}
          >
            {pwdVisibility ? <Visibility /> : <VisibilityOff />}
          </IconButton>
        </InputAdornment>
      }
      inputProps={{
        'data-cy': dataCy,
      }}
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
(StyledPasswordWrapper as any).getWidgetAttributes = () => {
  return {
    placeholder: { type: 'string', required: false },
  };
};
