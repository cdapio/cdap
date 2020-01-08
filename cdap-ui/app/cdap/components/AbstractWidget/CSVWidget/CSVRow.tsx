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
    disabled: {
      color: `${theme.palette.grey['50']}`,
    },
  };
};

interface ICSVRowProps extends IAbstractRowProps<typeof styles> {
  valuePlaceholder?: string;
}

interface ICSVRowState {
  value: string;
}

class CSVRow extends AbstractRow<ICSVRowProps, ICSVRowState> {
  public static defaultProps = {
    valuePlaceholder: 'Value',
  };

  public state = {
    value: this.props.value,
  };

  private handleChange = (e) => {
    const value = e.target.value;

    this.setState({
      value,
    });

    this.onChange(value);
  };

  public renderInput = () => {
    return (
      <Input
        classes={{ disabled: this.props.classes.disabled }}
        placeholder={this.props.valuePlaceholder}
        onChange={this.handleChange}
        value={this.state.value}
        autoFocus={this.props.autofocus}
        onKeyPress={this.handleKeyPress}
        onKeyDown={this.handleKeyDown}
        disabled={this.props.disabled}
        inputRef={this.props.forwardedRef}
        data-cy="key"
      />
    );
  };
}

const StyledCSVRow = withStyles(styles)(CSVRow);
export default StyledCSVRow;
