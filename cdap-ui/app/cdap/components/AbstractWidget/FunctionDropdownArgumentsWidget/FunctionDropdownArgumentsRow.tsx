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
import Input from '@material-ui/core/Input';
import Checkbox from '@material-ui/core/Checkbox';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';
import Select from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import AbstractRow, {
  IAbstractRowProps,
  AbstractRowStyles,
} from 'components/AbstractWidget/AbstractMultiRowWidget/AbstractRow';
import { parse, serialize } from 'components/AbstractWidget/FunctionDropdownArgumentsWidget/parser';

const styles = (theme): StyleRules => {
  return {
    ...AbstractRowStyles(theme),
    inputContainer: {
      display: 'grid',
      gridTemplateColumns: '1fr 1fr 1fr 1fr 30px 1fr',
      gridGap: '10px',
    },
    disabled: {
      color: `${theme.palette.grey['50']}`,
    },
    separator: {
      textAlign: 'center',
    },
  };
};

interface IComplexDropdown {
  value: string | number;
  label: string;
}

export type IDropdownOption = string | number | IComplexDropdown;

interface IFunctionDropdownArgumentsRowProps extends IAbstractRowProps<typeof styles> {
  placeholders: Record<string, string>;
  dropdownOptions: IDropdownOption[];
}

interface IKeyValueState {
  field: string;
  func: string;
  alias: string;
  arguments: string;
  ignoreNulls: boolean;
}

type StateKeys = keyof IKeyValueState;

class FunctionDropdownArgumentsRow extends AbstractRow<
  IFunctionDropdownArgumentsRowProps,
  IKeyValueState
> {
  public static defaultProps = {
    placeholders: {
      field: 'field',
      alias: 'alias',
      arguments: 'arguments',
    },
    dropdownOptions: [],
  };

  public state = {
    field: '',
    func: '',
    alias: '',
    arguments: '',
    ignoreNulls: true,
  };

  /**
   * Sample input: alias:Avg(fieldName)
   */
  public componentDidMount() {
    this.setState(parse(this.props.value));
  }

  private handleChange = (type: StateKeys, e) => {
    this.setState(
      {
        [type]: e.target.value,
      } as Pick<IKeyValueState, StateKeys>,
      () => {
        const { field, func, alias, arguments: args, ignoreNulls } = this.state;

        const updatedValue = serialize({
          field,
          func,
          alias,
          arguments: args,
          ignoreNulls,
        });

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
        <Select
          classes={{ disabled: this.props.classes.disabled }}
          value={this.state.func}
          onChange={this.handleChange.bind(this, 'func')}
          displayEmpty={true}
          disabled={this.props.disabled}
        >
          {dropdownOptions.map((option) => {
            return (
              <MenuItem value={option.value} key={option.value}>
                {option.label}
              </MenuItem>
            );
          })}
        </Select>

        <Input
          classes={{ disabled: this.props.classes.disabled }}
          placeholder={this.props.placeholders.field}
          onChange={this.handleChange.bind(this, 'field')}
          value={this.state.field}
          autoFocus={this.props.autofocus}
          onKeyPress={this.handleKeyPress}
          onKeyDown={this.handleKeyDown}
          disabled={this.props.disabled}
          inputRef={this.props.forwardedRef}
        />

        <Input
          classes={{ disabled: this.props.classes.disabled }}
          placeholder={this.props.placeholders.arguments}
          onChange={this.handleChange.bind(this, 'arguments')}
          value={this.state.arguments}
          onKeyPress={this.handleKeyPress}
          onKeyDown={this.handleKeyDown}
          disabled={this.props.disabled}
        />

        <FormControlLabel
          className={this.props.classes.checkbox}
          disabled={this.props.disabled}
          control={
            <Checkbox
              classes={{ disabled: this.props.classes.disabled }}
              onChange={this.handleChange.bind(this, 'ignoreNulls')}
              checked={this.state.ignoreNulls}
              disabled={this.props.disabled}
              color="primary"
            />
          }
          label={<span>Ignore Nulls</span>}
        />

        <span className={this.props.classes.separator}>as</span>

        <Input
          classes={{ disabled: this.props.classes.disabled }}
          placeholder={this.props.placeholders.alias}
          onChange={this.handleChange.bind(this, 'alias')}
          value={this.state.alias}
          onKeyPress={this.handleKeyPress}
          onKeyDown={this.handleKeyDown}
          disabled={this.props.disabled}
        />
      </div>
    );
  };
}

const StyledFunctionDropdownRow = withStyles(styles)(FunctionDropdownArgumentsRow);
export default StyledFunctionDropdownRow;
