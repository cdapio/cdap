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
import AbstractMultiRowWidget, {
  IMultiRowProps,
} from 'components/AbstractWidget/AbstractMultiRowWidget';
import TableRow from 'components/Replicator/Create/Content/SelectTables/ManualSelectTable/TableMultiRow/TableRow';

interface ITableMultiRowProps extends IMultiRowProps<{}> {
  value: any;
}

class TableMultiRow extends AbstractMultiRowWidget<ITableMultiRowProps> {
  public componentWillReceiveProps() {
    // overwrite
    // no-op
  }

  public deconstructValues = (props) => {
    if (!props.value || props.value.length === 0) {
      return [];
    }
    return props.value;
  };

  public constructValues = (): any => {
    const values = this.state.rows
      .filter((id) => this.values[id] && this.values[id].value)
      .map((id) => this.values[id].value);

    return values;
  };

  public renderRow = (id, index) => {
    return (
      <TableRow
        key={id}
        value={this.values[id].value}
        id={id}
        index={index}
        onChange={this.editRow}
        addRow={this.addRow.bind(this, index)}
        removeRow={this.removeRow.bind(this, index)}
        autofocus={this.state.autofocus === id}
        changeFocus={this.changeFocus}
        disabled={this.props.disabled}
        forwardedRef={this.values[id].ref}
        errors={this.props.errors}
      />
    );
  };
}

export default TableMultiRow;
