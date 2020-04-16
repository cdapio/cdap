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
import TextField from '@material-ui/core/TextField';
import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';
import AbstractRow, {
  IAbstractRowProps,
  AbstractRowStyles,
} from 'components/AbstractWidget/AbstractMultiRowWidget/AbstractRow';
import classnames from 'classnames';
const styles = (theme): StyleRules => {
  return {
    ...AbstractRowStyles(theme),
    inputContainer: {
      display: 'grid',
      gridTemplateColumns: '50% 50%',
      gridGap: '10px',
      // TODO: clean up this additional styling for legend after upgrading bootstrap
      // and verifying bootstrap does not add additional border-bottom.
      '& legend': {
        border: '0px',
      },
    },
    disabled: {
      '& .Mui-disabled': {
        cursor: 'not-allowed',
        color: `${theme.palette.grey['50']}`,
      },
    },
  };
};

interface IKeyValueState {
  value: string;
  key: string;
}
interface IRuntimeArgsRowProps extends IAbstractRowProps<typeof styles> {
  valuePlaceholder?: string;
  keyPlaceholder?: string;
  isEncoded: boolean;
  value: {
    key: string;
    value: string;
    notDeletable: boolean;
  };
}

type StateKeys = keyof IKeyValueState;

class RuntimeArgsRowView extends AbstractRow<IRuntimeArgsRowProps, IKeyValueState> {
  public state = {
    key: '',
    value: '',
  };
  public componentDidMount() {
    let { key, value } = this.props.value;
    if (this.props.isEncoded) {
      key = decodeURIComponent(key);
      value = decodeURIComponent(value);
    }
    this.setState({
      ...this.props.value,
      key,
      value,
    });
  }

  public handleChange = (type: StateKeys, e) => {
    this.setState(
      {
        [type]: e.target.value,
      } as Pick<IKeyValueState, StateKeys>,
      () => {
        let { key, value } = this.state;
        if (this.props.isEncoded) {
          key = encodeURIComponent(key);
          value = encodeURIComponent(value);
        }
        this.onChange({ ...this.props.value, key, value });
      }
    );
  };

  public renderInput = () => {
    const keyDisabled = this.props.disabled || this.props.value.notDeletable;

    return (
      <div className={this.props.classes.inputContainer}>
        <TextField
          data-cy="runtimeargs-key"
          label={this.props.keyPlaceholder}
          onChange={this.handleChange.bind(this, 'key')}
          value={this.state.key}
          autoFocus={this.props.autofocus}
          onKeyPress={this.handleKeyPress}
          onKeyDown={this.handleKeyDown}
          disabled={keyDisabled}
          inputRef={this.props.forwardedRef}
          variant="outlined"
          margin="dense"
          className={classnames({ [this.props.classes.disabled]: keyDisabled })}
        />

        <TextField
          data-cy="runtimeargs-value"
          label={this.props.valuePlaceholder}
          onChange={this.handleChange.bind(this, 'value')}
          value={this.state.value}
          onKeyPress={this.handleKeyPress}
          onKeyDown={this.handleKeyDown}
          disabled={this.props.disabled}
          variant="outlined"
          margin="dense"
          className={classnames({ [this.props.classes.disabled]: this.props.disabled })}
        />
      </div>
    );
  };
}
const RuntimeArgsRow = withStyles(styles)(RuntimeArgsRowView);
export default RuntimeArgsRow;
