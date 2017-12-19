/*
 * Copyright © 2017 Cask Data, Inc.
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

import React from 'react';
import SortableTable from 'components/SortableTable';
import classnames from 'classnames';

export default class SortableStickyTable extends SortableTable {
  constructor(props) {
    super(props);
  }
  renderTableHeader() {
    return (
      <thead>
        <tr>
          {
            this.props.tableHeaders.map((tableHeader, i) => {
              return (
                <th key={i}>
                  {
                    tableHeader.property ?
                      this.renderSortableTableHeader(tableHeader)
                    :
                      tableHeader.label
                  }
                </th>
              );
            })
          }
        </tr>
      </thead>
    );
  }
  render() {
    let tableClasses = classnames('table', this.props.className);
    return (
      <div className="table-container">
        <table className={tableClasses}>
          {this.renderTableHeader()}
        </table>
        <div className="table-scroll">
          {this.renderTableBody(this.state.entities)}
        </div>
      </div>
    );
  }
}

SortableStickyTable.propTypes = {
  ...SortableTable.propTypes
};
