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

import React from 'react';
import ThemeWrapper from 'components/ThemeWrapper';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import TextField from '@material-ui/core/TextField';
import { IWidgetProps } from 'components/AbstractWidget';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import { objectQuery } from 'services/helpers';

const styles = (theme) => {
  return {
    input: {
      padding: 10,
    },
    root: {
      '&$focused $notchedOutline': {
        border: `1px solid ${theme.palette.blue[100]}`,
      },
    },
    focused: {},
    notchedOutline: {},
  };
};

interface INumberWidgetProps extends WithStyles<typeof styles> {
  min?: number;
  max?: number;
}

interface INumberProps extends IWidgetProps<INumberWidgetProps>, WithStyles<typeof styles> {}

const NumberView: React.FC<INumberProps> = ({
  value,
  onChange,
  disabled,
  widgetProps,
  classes,
}) => {
  const onChangeHandler = (event: React.ChangeEvent<HTMLInputElement>) => {
    const v = event.target.value;
    if (typeof onChange === 'function') {
      onChange(v);
    }
  };

  const min = objectQuery(widgetProps, 'min') || Number.MIN_SAFE_INTEGER;
  const max = objectQuery(widgetProps, 'max') || Number.MAX_SAFE_INTEGER;

  return (
    <TextField
      fullWidth
      variant="outlined"
      type="number"
      value={value}
      onChange={onChangeHandler}
      disabled={disabled}
      inputProps={{
        min,
        max,
      }}
      InputProps={{
        classes,
      }}
    />
  );
};

const StyledNumber = withStyles(styles)(NumberView);

export default function StyledNumberWrapper(props) {
  return (
    <ThemeWrapper>
      <StyledNumber {...props} />
    </ThemeWrapper>
  );
}

(StyledNumberWrapper as any).propTypes = WIDGET_PROPTYPES;
