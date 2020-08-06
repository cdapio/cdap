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

import { IWidgetProps } from 'components/AbstractWidget';
import MultiRowContainer from 'components/AbstractWidget/AbstractMultiRowWidget/Container';
import { objectQuery } from 'services/helpers';
import uuidV4 from 'uuid/v4';

export interface IMultiRowWidgetProps {
  delimiter?: string;
}

export interface IMultiRowProps<W extends IMultiRowWidgetProps> extends IWidgetProps<W> {}

interface IMultiRowState {
  rows: string[];
  autofocus?: string;
}

export default class AbstractMultiRowWidget<
  W extends IWidgetProps<IMultiRowWidgetProps>
> extends React.PureComponent<W, IMultiRowState> {
  public state = {
    rows: [],
    autofocus: null,
  };

  public values = {};

  public componentDidMount() {
    this.init(this.props);
  }

  /**
   * TODO: componentWillReceiveProps is already considered UNSAFE operation in React. Will have
   * to come up with a different approach to update the component based on an async changes.
   */
  public componentWillReceiveProps(nextProps) {
    const currentValue = this.constructValues();

    if (currentValue === nextProps.value) {
      return;
    }

    this.init(nextProps);
  }

  public deconstructValues = (props) => {
    if (!props.value || props.value.length === 0) {
      return [];
    }
    const delimiter = objectQuery(props, 'widgetProps', 'delimiter') || ',';

    return props.value.split(delimiter);
  };

  private init = (props) => {
    const splitValues = this.deconstructValues(props);

    if (splitValues.length === 0) {
      // reset state before adding a new empty row
      this.values = {};
      this.setState(
        {
          rows: [],
          autofocus: null,
        },
        () => {
          if (!this.state.rows || this.state.rows.length === 0) {
            // adding a new empty row in case values are empty
            this.addRow(-1, false);
          }
        }
      );

      return;
    }

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
  };

  public addRow = (index = -1, shouldFocus: boolean = true) => {
    const rows = this.state.rows.slice();
    const id = uuidV4();
    rows.splice(index + 1, 0, id);

    this.values[id] = {
      ref: React.createRef(),
      value: '',
    };

    this.setState(
      {
        rows,
        autofocus: shouldFocus ? id : null,
      },
      () => {
        if (shouldFocus) {
          this.onChange();
        }
      }
    );
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
  // Need any here to allow constructValues in extended classes
  // to return types other than string
  public constructValues = (): any => {
    const delimiter = objectQuery(this.props, 'widgetProps', 'delimiter') || ',';

    const values = this.state.rows
      .filter((id) => this.values[id] && this.values[id].value)
      .map((id) => this.values[id].value)
      .join(delimiter);

    return values;
  };

  public onChange = () => {
    if (this.props.onChange) {
      this.props.onChange(this.constructValues());
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
      <MultiRowContainer dataCy={this.props.dataCy}>
        {this.state.rows.map((id, index) => {
          if (!this.values[id]) {
            return null;
          }
          return this.renderRow(id, index);
        })}
      </MultiRowContainer>
    );
  }
}
