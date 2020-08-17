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
import TextBox from 'components/AbstractWidget/FormInputs/TextBox';
import Checkbox from '@material-ui/core/Checkbox';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';
import Select from 'components/AbstractWidget/FormInputs/Select';
import MenuItem from '@material-ui/core/MenuItem';
import AbstractRow, {
  IAbstractRowProps,
  AbstractRowStyles,
} from 'components/AbstractWidget/AbstractMultiRowWidget/AbstractRow';
import { parse, serialize } from 'components/AbstractWidget/FunctionDropdownArgumentsWidget/parser';
import InputFieldDropdown from 'components/AbstractWidget/InputFieldDropdown';
import If from 'components/If';
import { IWidgetExtraConfig } from 'components/AbstractWidget';

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
    // Align with surrounding controls
    separator: {
      paddingTop: '10px',
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
  extraConfig?: IWidgetExtraConfig;
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
      func: 'function',
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

  private handleCheckboxChange = (type: StateKeys, e) => {
    this.setState(
      {
        [type]: e.target.checked,
      } as Pick<IKeyValueState, StateKeys>,
      this.serializeChange
    );
  };

  private handleWidgetChange = (type: StateKeys, value) => {
    this.setState(
      {
        [type]: value,
      } as Pick<IKeyValueState, StateKeys>,
      this.serializeChange
    );
  };

  private serializeChange = () => {
    const { field, func, alias, arguments: args, ignoreNulls } = this.state;

    const updatedValue = serialize({
      field,
      func,
      alias,
      arguments: args,
      ignoreNulls,
    });

    this.onChange(updatedValue);
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
          value={this.state.func}
          onChange={this.handleWidgetChange.bind(this, 'func')}
          disabled={this.props.disabled}
          placeholder={this.props.placeholders.func}
          widgetProps={{
            options: dropdownOptions,
            enableUnderline: true,
          }}
        />

        <If condition={!!this.props.extraConfig && !!this.props.extraConfig.inputSchema}>
          <InputFieldDropdown
            disabled={this.props.disabled}
            extraConfig={this.props.extraConfig}
            onChange={this.handleWidgetChange.bind(this, 'field')}
            onKeyPress={this.handleKeyPress}
            value={this.state.field}
            widgetProps={{
              enableUnderline: true,
            }}
          />
        </If>

        <If condition={!this.props.extraConfig || !this.props.extraConfig.inputSchema}>
          <TextBox
            onChange={this.handleWidgetChange.bind(this, 'field')}
            value={this.state.field}
            autoFocus={this.props.autofocus}
            onKeyPress={this.handleKeyPress}
            disabled={this.props.disabled}
            inputRef={this.props.forwardedRef}
            widgetProps={{
              placeholder: this.props.placeholders.field,
            }}
          />
        </If>

        <TextBox
          onChange={this.handleWidgetChange.bind(this, 'arguments')}
          value={this.state.arguments}
          onKeyPress={this.handleKeyPress}
          disabled={this.props.disabled}
          widgetProps={{
            placeholder: this.props.placeholders.arguments,
            enableUnderline: true,
          }}
        />

        <FormControlLabel
          className={this.props.classes.checkbox}
          disabled={this.props.disabled}
          control={
            <Checkbox
              classes={{ disabled: this.props.classes.disabled }}
              onChange={this.handleCheckboxChange.bind(this, 'ignoreNulls')}
              checked={this.state.ignoreNulls}
              disabled={this.props.disabled}
              color="primary"
            />
          }
          label={<span>Ignore Nulls</span>}
        />

        <span className={this.props.classes.separator}>as</span>

        <TextBox
          onChange={this.handleWidgetChange.bind(this, 'alias')}
          value={this.state.alias}
          onKeyPress={this.handleKeyPress}
          disabled={this.props.disabled}
          widgetProps={{
            placeholder: this.props.placeholders.alias,
            enableUnderline: true,
          }}
        />
      </div>
    );
  };
}

const StyledFunctionDropdownRow = withStyles(styles)(FunctionDropdownArgumentsRow);
export default StyledFunctionDropdownRow;
