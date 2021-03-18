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
import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';
import Select from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import If from 'components/If';
import AbstractRow, {
  IAbstractRowProps,
  AbstractRowStyles,
} from 'components/AbstractWidget/AbstractMultiRowWidget/AbstractRow';

const styles = (theme): StyleRules => {
  return {
    ...AbstractRowStyles(theme),
    inputContainerWithCondition: {
      display: 'grid',
      gridTemplateColumns: '0.80fr 0.80fr 0.80fr 30px 0.80fr',
      gridGap: '20px',
    },
    inputContainer: {
      display: 'grid',
      gridTemplateColumns: '1fr 1fr 30px 1fr',
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
  hasCondition?: boolean;
  value: string | number;
  label: string;
}

export type IDropdownOption = string | number | IComplexDropdown;

interface IFunctionDropdownRowProps extends IAbstractRowProps<typeof styles> {
  placeholders: Record<string, string>;
  dropdownOptions: IDropdownOption[];
}

interface IKeyValueState {
  field: string;
  func: string;
  alias: string;
  hasCondition: boolean;
  condition: string;
}

type StateKeys = keyof IKeyValueState;

class FunctionDropdownRow extends AbstractRow<IFunctionDropdownRowProps, IKeyValueState> {
  public static defaultProps = {
    placeholders: {
      field: 'field',
      alias: 'alias',
      condition: 'condition',
    },
    dropdownOptions: [],
  };

  public state = {
    field: '',
    func: '',
    alias: '',
    hasCondition: false,
    condition: '',
  };

  /**
   * Sample input: alias:Avg(fieldName)
   */
  public componentDidMount() {
    const [alias, fn, conditionValue] = this.props.value.split(':');
    const { func, field, hasCondition, condition } = this.extractFunctionAliasAndCondition(
      fn,
      conditionValue
    );

    this.setState({
      field,
      func,
      alias,
      hasCondition,
      condition,
    });
  }

  private extractFunctionAliasAndCondition(fn, conditionValue) {
    const defaultResponse = {
      func: '',
      field: '',
      hasCondition: false,
      condition: '',
    };

    if (!fn) {
      return defaultResponse;
    }

    const fnOpenBracketIndex = fn.indexOf('(');
    const fnCloseBracketIndex = fn.indexOf(')');
    const conditionOpenBracketIndex = conditionValue.indexOf('(');
    const conditionCloseBracketIndex = conditionValue.indexOf(')');

    if (fnOpenBracketIndex === -1 || fnCloseBracketIndex === -1) {
      return defaultResponse;
    }

    if (conditionValue) {
      return {
        func: fn.substring(0, fnOpenBracketIndex),
        field: fn.substring(fnOpenBracketIndex + 1, fnCloseBracketIndex),
        hasCondition: true,
        condition: conditionValue.substring(
          conditionOpenBracketIndex + 1,
          conditionCloseBracketIndex + 1
        ),
      };
    }

    return {
      func: fn.substring(0, fnOpenBracketIndex),
      field: fn.substring(fnOpenBracketIndex + 1, fnCloseBracketIndex),
      hasCondition: false,
      condition: '',
    };
  }

  private handleCondition = (type: StateKeys, e) => {
    if (type === 'func') {
      const targetValueObj = this.props.dropdownOptions.filter(
        (el) => typeof el === 'object' && el.value === e.target.value
      );
      if (targetValueObj.length !== 0) {
        this.setState({
          hasCondition: targetValueObj[0].hasOwnProperty('hasCondition'),
        });
      } else {
        this.setState({
          hasCondition: false,
        });
      }
    }
  };

  private handleChange = (type: StateKeys, e) => {
    this.handleCondition(type, e);
    this.setState(
      {
        [type]: e.target.value,
      } as Pick<IKeyValueState, StateKeys>,
      () => {
        const { field, func, alias, condition, hasCondition } = this.state;
        let updatedValue = '';

        if (field.length === 0 || func.length === 0 || alias.length === 0) {
          this.onChange('');
          return;
        }

        if (hasCondition) {
          updatedValue = `${alias}:${func}(${field}):condition(${condition})`;
          this.onChange(updatedValue);
          return;
        }
        updatedValue = `${alias}:${func}(${field})`;
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
      <div
        className={
          !this.state.hasCondition
            ? this.props.classes.inputContainer
            : this.props.classes.inputContainerWithCondition
        }
      >
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
        <If condition={this.state.hasCondition}>
          <Input
            classes={{ disabled: this.props.classes.disabled }}
            placeholder={this.props.placeholders.condition}
            onChange={this.handleChange.bind(this, 'condition')}
            value={this.state.condition}
            onKeyPress={this.handleKeyPress}
            onKeyDown={this.handleKeyDown}
            disabled={this.props.disabled}
          />
        </If>
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

const StyledFunctionDropdownRow = withStyles(styles)(FunctionDropdownRow);
export default StyledFunctionDropdownRow;
