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
import IconButton from '@material-ui/core/IconButton';
import AddIcon from '@material-ui/icons/Add';
import DeleteIcon from '@material-ui/icons/Delete';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import If from 'components/If';

enum KEY_CODE {
  Enter = 13,
  Up = 38,
  Down = 40,
}

const styles = (theme) => {
  return {
    root: {
      height: '44px',
    },
    input: {
      width: 'calc(100% - 100px)',
      'margin-right': '10px',
    },
    disabled: {
      color: '#333333',
    },
  };
};

interface IProps extends WithStyles<typeof styles> {
  value: string;
  id: string;
  index: number;
  autofocus: boolean;
  disabled: boolean;
  valuePlaceholder?: string;
  onChange: (id: string, value: string) => void;
  addRow: () => void;
  removeRow: () => void;
  changeFocus: (index: number) => void;
}

interface IState {
  value: string;
}

class CSVRow extends React.PureComponent<IProps, IState> {
  public static defaultProps = {
    valuePlaceholder: 'Value',
  };

  public state = {
    value: this.props.value,
  };

  private onChange = (e) => {
    const value = e.target.value;

    this.setState({
      value,
    });

    this.props.onChange(this.props.id, value);
  };

  private handleKeyPress = (e) => {
    if (e.nativeEvent.keyCode !== KEY_CODE.Enter) {
      return;
    }

    this.props.addRow();
  };

  private handleKeyDown = (e) => {
    switch (e.nativeEvent.keyCode) {
      case KEY_CODE.Up:
        e.preventDefault();
        this.props.changeFocus(this.props.index - 1);
        return;
      case KEY_CODE.Down:
        this.props.changeFocus(this.props.index + 1);
        return;
    }
  };

  public render() {
    return (
      <div className={this.props.classes.root}>
        <Input
          id={`csv-row-${this.props.id}`}
          className={this.props.classes.input}
          classes={{ disabled: this.props.classes.disabled }}
          placeholder={this.props.valuePlaceholder}
          onChange={this.onChange}
          value={this.state.value}
          autoFocus={this.props.autofocus}
          onKeyPress={this.handleKeyPress}
          onKeyDown={this.handleKeyDown}
          disabled={this.props.disabled}
        />

        <If condition={!this.props.disabled}>
          <React.Fragment>
            <IconButton onClick={this.props.addRow}>
              <AddIcon fontSize="small" />
            </IconButton>

            <IconButton color="secondary" onClick={this.props.removeRow}>
              <DeleteIcon fontSize="small" />
            </IconButton>
          </React.Fragment>
        </If>
      </div>
    );
  }
}

const StyledCSVRow = withStyles(styles)(CSVRow);
export default StyledCSVRow;
