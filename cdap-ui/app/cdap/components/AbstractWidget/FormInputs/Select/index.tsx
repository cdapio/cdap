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

import PropTypes from 'prop-types';
import Select from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import withStyles from '@material-ui/core/styles/withStyles';
import InputBase from '@material-ui/core/InputBase';

const styles = () => {
  return {
    input: {
      borderRadius: 4,
      position: 'relative' as 'relative',
      border: '1px solid #ced4da',
      padding: '10px 18px 10px 12px',
      '&:focus': {
        backgroundColor: 'transparent',
        borderRadius: 4,
      },
    },
  };
};
const CustomizedInput = withStyles(styles)(InputBase);

interface ISelectOptions {
  value: string | number; // We need to expand this when we have complex use cases
  label: string;
}
interface ISelectProps {
  value: string;
  onChange: (value: string) => void;
  options: ISelectOptions[];
  disabled?: boolean;
}

export default function CustomSelect({ value, onChange, options, disabled }: ISelectProps) {
  const onChangeHandler = (event: React.ChangeEvent<HTMLSelectElement>) => {
    const v = event.target.value;
    if (typeof onChange === 'function') {
      onChange(v);
    }
  };
  const optionValues = options.map((opt) => {
    return typeof opt === 'string' ? { value: opt, label: opt } : opt;
  });
  return (
    <Select
      fullWidth
      value={value}
      onChange={onChangeHandler}
      input={<CustomizedInput />}
      disabled={disabled}
    >
      {optionValues.map((opt) => (
        <MenuItem value={opt.value} key={opt.value}>
          {opt.label}
        </MenuItem>
      ))}
    </Select>
  );
}

(CustomSelect as any).propTypes = {
  value: PropTypes.string,
  onChange: PropTypes.func,
  options: PropTypes.arrayOf(PropTypes.string),
  disabled: PropTypes.bool,
};
