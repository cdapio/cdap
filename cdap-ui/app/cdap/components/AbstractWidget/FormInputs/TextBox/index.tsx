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
import PropTypes from 'prop-types';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';

const styles = (theme) => {
  return {
    ':hover': {
      borderColor: theme.palette.grey['300'],
    },
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
interface ITextBoxProps extends WithStyles<typeof styles> {
  value: string;
  onChange: (value: string) => void;
  placeholder: string;
}

function TextBox({ value, onChange, placeholder, classes }: ITextBoxProps) {
  const onChangeHandler = (event: React.ChangeEvent<HTMLInputElement>) => {
    const v = event.target.value;
    if (typeof onChange === 'function') {
      onChange(v);
    }
  };
  return (
    <TextField
      fullWidth
      variant="outlined"
      value={value}
      onChange={onChangeHandler}
      className={classes.root}
      placeholder={placeholder}
      InputProps={{
        classes,
      }}
    />
  );
}
const StyledTextBox = withStyles(styles)(TextBox);
export default function TextBoxWrapper(props) {
  return (
    <ThemeWrapper>
      <StyledTextBox {...props} />
    </ThemeWrapper>
  );
}
(TextBoxWrapper as any).propTypes = {
  value: PropTypes.string,
  onChange: PropTypes.func,
  placeholder: PropTypes.string,
};
