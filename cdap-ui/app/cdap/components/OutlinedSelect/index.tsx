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
import Select from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';
import InputBase from '@material-ui/core/InputBase';

const CustomizedInput = withStyles(
  (theme): StyleRules => {
    return {
      input: {
        borderRadius: '4px',
        border: `1px solid ${theme.palette.grey[300]}`,
        padding: '10px 18px 10px 12px',
        '&:focus': {
          backgroundColor: 'transparent',
          borderRadius: '4px',
        },
      },
    };
  }
)(InputBase);

interface IOption {
  label: string;
  value: string;
}

interface IOutlinedSelectProps {
  options: Array<IOption | string>;
  value: string;
  onChange: (value: string) => void;
  disabled: boolean;
}

const OutlinedSelect: React.FC<IOutlinedSelectProps> = ({ options, value, onChange, disabled }) => {
  const dropdownOptions = options.map((opt: any) => {
    if (typeof opt === 'string') {
      return {
        label: opt,
        value: opt,
      };
    }

    return opt;
  });

  function handleOnChange(e) {
    const val = e.target.value;
    onChange(val);
  }

  return (
    <Select
      fullWidth
      value={value}
      onChange={handleOnChange}
      disabled={disabled}
      input={<CustomizedInput />}
      title={value}
    >
      {dropdownOptions.map((opt) => {
        return (
          <MenuItem value={opt.value} key={opt.value}>
            {opt.label}
          </MenuItem>
        );
      })}
    </Select>
  );
};

export default OutlinedSelect;
