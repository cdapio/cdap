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
import TextField from '@material-ui/core/TextField';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';
import { objectQuery } from 'services/helpers';
import { IWidgetProps } from 'components/AbstractWidget';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';

const styles = (theme) => {
  return {
    input: {
      padding: 10,
    },
    root: {
      '& $notchedOutline': {
        borderColor: theme.palette.grey['300'],
      },
      '&:hover $notchedOutline': {
        borderColor: theme.palette.grey['300'],
      },
      '&$focused $notchedOutline': {
        border: `1px solid ${theme.palette.blue[100]}`,
      },
    },
    focused: {},
    notchedOutline: {},
  };
};

interface ITextBoxWidgetProps {
  placeholder?: string;
}

interface ITextBoxProps extends IWidgetProps<ITextBoxWidgetProps>, WithStyles<typeof styles> {}

const TextBox: React.FC<ITextBoxProps> = ({ value, onChange, widgetProps, disabled, classes }) => {
  const onChangeHandler = (event: React.ChangeEvent<HTMLInputElement>) => {
    const v = event.target.value;
    if (typeof onChange === 'function') {
      onChange(v);
    }
  };

  const placeholder = objectQuery(widgetProps, 'placeholder');

  return (
    <TextField
      fullWidth
      variant="outlined"
      value={value}
      onChange={onChangeHandler}
      className={classes.root}
      placeholder={placeholder}
      disabled={disabled}
      InputProps={{
        classes,
      }}
    />
  );
};
const StyledTextBox = withStyles(styles)(TextBox);
export default function TextBoxWrapper(props) {
  return (
    <ThemeWrapper>
      <StyledTextBox {...props} />
    </ThemeWrapper>
  );
}

(TextBoxWrapper as any).propTypes = WIDGET_PROPTYPES;
