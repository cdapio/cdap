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

interface IKeyValueRowProps extends IAbstractRowProps<typeof styles> {
  valuePlaceholder?: string;
  keyPlaceholder?: string;
  kvDelimiter?: string;
  isEncoded: boolean;
}

interface IKeyValueState {
  value: string;
  key: string;
}

type StateKeys = keyof IKeyValueState;

export class KeyValueRow extends AbstractRow<IKeyValueRowProps, IKeyValueState> {
  public static defaultProps = {
    keyPlaceholder: 'Key',
    valuePlaceholder: 'Value',
    kvDelimiter: ':',
    isEncoded: false,
  };

  public state = {
    key: '',
    value: '',
  };

  public componentDidMount() {
    let [key = '', value = ''] = this.props.value.split(this.props.kvDelimiter);

    if (this.props.isEncoded) {
      key = decodeURIComponent(key);
      value = decodeURIComponent(value);
    }

    this.setState({
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
        let key = this.state.key;
        let value = this.state.value;

        if (this.props.isEncoded) {
          key = encodeURIComponent(key);
          value = encodeURIComponent(value);
        }

        const updatedValue = key.length > 0 ? [key, value].join(this.props.kvDelimiter) : '';
        this.onChange(updatedValue);
      }
    );
  };

  public renderInput = () => {
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

        <Input
          classes={{ disabled: this.props.classes.disabled }}
          placeholder={this.props.valuePlaceholder}
          onChange={this.handleChange.bind(this, 'value')}
          value={this.state.value}
          onKeyPress={this.handleKeyPress}
          onKeyDown={this.handleKeyDown}
          disabled={this.props.disabled}
          data-cy="value"
        />
      </div>
    );
  };
}

const StyledKeyValueRow = withStyles(styles)(KeyValueRow);
export default StyledKeyValueRow;
