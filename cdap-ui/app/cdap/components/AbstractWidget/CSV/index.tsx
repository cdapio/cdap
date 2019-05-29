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
import PropTypes from 'prop-types';
import uuidV4 from 'uuid/v4';
import CSVRow from 'components/AbstractWidget/CSV/CSVRow';
import ThemeWrapper from 'components/ThemeWrapper';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';

const styles = () => {
  return {
    csvContainer: {
      padding: '10px',
      border: '2px solid #cccccc',
      'border-radius': '4px',
    },
  };
};

interface ICSVWidgetProps extends WithStyles<typeof styles> {
  onChange: (values: string) => void;
  value: string;
  disabled: boolean;
  valuePlaceholder?: string;
  delimiter?: string;
}

interface ICSVWidgetState {
  rows: string[];
  autofocus?: string;
}

class CSVWidget extends React.PureComponent<ICSVWidgetProps, ICSVWidgetState> {
  public static defaultProps = {
    delimiter: ',',
  };

  public state = {
    rows: [],
    autofocus: null,
  };

  private values = {};

  public componentDidMount() {
    if (!this.props.value || this.props.value.length === 0) {
      this.addRow();
      return;
    }

    const delimiter = this.props.delimiter;

    const splitValues = this.props.value.split(delimiter);
    const rows = [];

    splitValues.forEach((value) => {
      const id = uuidV4();
      this.values[id] = value;

      rows.push(id);
    });

    this.setState({ rows });
  }

  private addRow = (index = -1) => {
    const rows = this.state.rows.slice();
    const id = uuidV4();
    rows.splice(index + 1, 0, id);

    this.values[id] = '';

    this.setState({
      rows,
      autofocus: id,
    });

    this.onChange();
  };

  private removeRow = (index) => {
    const rows = this.state.rows.slice();
    const id = rows[index];

    rows.splice(index, 1);

    this.setState(
      {
        rows,
      },
      () => {
        if (rows.length === 0) {
          this.addRow();
        }
      }
    );

    delete this.values[id];

    this.onChange();
  };

  private editRow = (id, value) => {
    this.values[id] = value;

    this.onChange();
  };

  private onChange = () => {
    const values = this.state.rows
      .filter((id) => this.values[id])
      .map((id) => this.values[id])
      .join(this.props.delimiter);

    if (this.props.onChange) {
      this.props.onChange(values);
    }
  };

  private changeFocus = (index) => {
    if (index < 0 || index > this.state.rows.length - 1) {
      return;
    }

    const focusId = this.state.rows[index];
    const elem = document.getElementById(`csv-row-${focusId}`) as HTMLInputElement;

    elem.focus();
  };

  public render() {
    return (
      <div className={this.props.classes.csvContainer}>
        {this.state.rows.map((id, index) => {
          return (
            <CSVRow
              key={id}
              value={this.values[id]}
              id={id}
              index={index}
              onChange={this.editRow}
              addRow={this.addRow.bind(this, index)}
              removeRow={this.removeRow.bind(this, index)}
              autofocus={this.state.autofocus === id}
              changeFocus={this.changeFocus}
              disabled={this.props.disabled}
              valuePlaceholder={this.props.valuePlaceholder}
            />
          );
        })}
      </div>
    );
  }
}

const StyledCSVWidget = withStyles(styles)(CSVWidget);

export default function StyledCSVWidgetWrapper(props) {
  return (
    <ThemeWrapper>
      <StyledCSVWidget {...props} />
    </ThemeWrapper>
  );
}

(StyledCSVWidgetWrapper as any).propTypes = {
  value: PropTypes.string,
  onChange: PropTypes.func,
  disabled: PropTypes.bool,
  delimiter: PropTypes.string,
  valuePlaceholder: PropTypes.string,
};
