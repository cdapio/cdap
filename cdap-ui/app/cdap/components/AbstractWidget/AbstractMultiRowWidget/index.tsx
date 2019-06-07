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
import uuidV4 from 'uuid/v4';
import MultiRowContainer from 'components/AbstractWidget/AbstractMultiRowWidget/Container';

export interface IMultiRowProps {
  onChange: (values: string) => void;
  value: string;
  disabled: boolean;
  delimiter?: string;
}

interface IMultiRowState {
  rows: string[];
  autofocus?: string;
}

export default class AbstractMultiRowWidget<P extends IMultiRowProps> extends React.PureComponent<
  P,
  IMultiRowState
> {
  public static defaultProps = {
    delimiter: ',',
  };

  public state = {
    rows: [],
    autofocus: null,
  };

  public values = {};

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
      this.values[id] = {
        ref: React.createRef(),
        value,
      };

      rows.push(id);
    });

    this.setState({ rows });
  }

  public addRow = (index = -1) => {
    const rows = this.state.rows.slice();
    const id = uuidV4();
    rows.splice(index + 1, 0, id);

    this.values[id] = {
      ref: React.createRef(),
      value: '',
    };

    this.setState({
      rows,
      autofocus: id,
    });

    this.onChange();
  };

  public removeRow = (index) => {
    const rows = this.state.rows.slice();
    const id = rows[index];

    rows.splice(index, 1);

    this.setState(
      {
        rows,
      },
      () => {
        delete this.values[id];
        if (rows.length === 0) {
          this.addRow();
        }
        this.onChange();
      }
    );
  };

  public editRow = (id, value) => {
    this.values[id].value = value;

    this.onChange();
  };

  public onChange = () => {
    const values = this.state.rows
      .filter((id) => this.values[id].value)
      .map((id) => this.values[id].value)
      .join(this.props.delimiter);

    if (this.props.onChange) {
      this.props.onChange(values);
    }
  };

  public changeFocus = (index) => {
    if (index < 0 || index > this.state.rows.length - 1) {
      return;
    }

    const focusId = this.state.rows[index];
    if (this.values[focusId].ref.current) {
      this.values[focusId].ref.current.focus();
    }
  };

  public renderRow = (id, index) => {
    return null;
  };

  public render() {
    return (
      <MultiRowContainer>
        {this.state.rows.map((id, index) => {
          return this.renderRow(id, index);
        })}
      </MultiRowContainer>
    );
  }
}
