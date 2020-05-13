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

interface IColumnRowState {
  value: string;
}

class ColumnRowView extends AbstractRow<IAbstractRowProps<typeof styles>, IColumnRowState> {
  public state = {
    value: this.props.value,
  };

  private handleChange = (e) => {
    const value = e.target.value;

    this.setState({
      value,
    });
  };

  private handleBlur = (e) => {
    this.onChange(e.target.value);
  };

  public renderInput = () => {
    return (
      <Input
        placeholder="Column name"
        onChange={this.handleChange}
        onBlur={this.handleBlur}
        value={this.state.value}
        autoFocus={this.props.autofocus}
        onKeyPress={this.handleKeyPress}
        onKeyDown={this.handleKeyDown}
        inputRef={this.props.forwardedRef}
      />
    );
  };
}

const ColumnRow = withStyles(styles)(ColumnRowView);
export default ColumnRow;
