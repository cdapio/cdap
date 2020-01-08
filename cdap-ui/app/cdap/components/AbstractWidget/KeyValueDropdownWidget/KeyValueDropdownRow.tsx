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
import Input from '@material-ui/core/Input';
import withStyles from '@material-ui/core/styles/withStyles';
import Select from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import AbstractRow, {
  IAbstractRowProps,
  AbstractRowStyles,
} from 'components/AbstractWidget/AbstractMultiRowWidget/AbstractRow';

const styles = (theme) => {
  return {
    ...AbstractRowStyles(theme),
    inputContainer: {
      display: 'grid',
      gridTemplateColumns: '50% 50%',
      gridGap: '10px',
    },
    disabled: {
      color: `${theme.palette.grey['50']}`,
    },
  };
};

interface IComplexDropdown {
  value: string | number;
  label: string;
}

export type IDropdownOption = string | number | IComplexDropdown;

interface IKeyValueDropdownRowProps extends IAbstractRowProps<typeof styles> {
  keyPlaceholder?: string;
  kvDelimiter?: string;
  dropdownOptions: IDropdownOption[];
}

interface IKeyValueState {
  value: string;
  key: string;
}

type StateKeys = keyof IKeyValueState;

class KeyValueDropdownRow extends AbstractRow<IKeyValueDropdownRowProps, IKeyValueState> {
  public static defaultProps = {
    keyPlaceholder: 'Key',
    kvDelimiter: ':',
    dropdownOptions: [],
  };

  public state = {
    key: '',
    value: '',
  };

  public componentDidMount() {
    const [key = '', value = ''] = this.props.value.split(this.props.kvDelimiter);

    this.setState({
      key,
      value,
    });
  }

  private handleChange = (type: StateKeys, e) => {
    this.setState(
      {
        [type]: e.target.value,
      } as Pick<IKeyValueState, StateKeys>,
      () => {
        const key = this.state.key;
        const value = this.state.value;

        const updatedValue = key.length > 0 ? [key, value].join(this.props.kvDelimiter) : '';
        this.onChange(updatedValue);
      }
    );
  };

  public renderInput = () => {
    const dropdownOptions = this.props.dropdownOptions.map((option: IDropdownOption) => {
      if (typeof option === 'object') {
        return option;
      }

      return {
        label: option.toString(),
        value: option,
      };
    });

    return (
      <div className={this.props.classes.inputContainer}>
        <Input
          classes={{ disabled: this.props.classes.disabled }}
          placeholder={this.props.keyPlaceholder}
          onChange={this.handleChange.bind(this, 'key')}
          value={this.state.key}
          autoFocus={this.props.autofocus}
          onKeyPress={this.handleKeyPress}
          onKeyDown={this.handleKeyDown}
          disabled={this.props.disabled}
          inputRef={this.props.forwardedRef}
          data-cy="key"
        />

        <Select
          classes={{ disabled: this.props.classes.disabled }}
          value={this.state.value}
          onChange={this.handleChange.bind(this, 'value')}
          displayEmpty={true}
          disabled={this.props.disabled}
          data-cy="value"
        >
          {dropdownOptions.map((option) => {
            return (
              <MenuItem value={option.value} key={option.value} data-cy={`value-${option.value}`}>
                {option.label}
              </MenuItem>
            );
          })}
        </Select>
      </div>
    );
  };
}

const StyledKeyValueDropdownRow = withStyles(styles)(KeyValueDropdownRow);
export default StyledKeyValueDropdownRow;
