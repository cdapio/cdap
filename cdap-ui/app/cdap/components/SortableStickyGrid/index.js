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
import PropTypes from 'prop-types';
import shortid from 'shortid';

require('./SortableStickyGrid.scss');

export default class SortableStickyGrid extends SortableTable {
  constructor(props) {
    super(props);
  }
  renderTableHeader() {
    if (this.props.renderTableHeaders) {
      return this.props.renderTableHeaders(this.renderSortableTableHeader.bind(this));
    }
    let itemWidth = 100 / this.props.tableHeaders.length;
    return (
      <div className="grid-header">
      {
        this.props.tableHeaders.map((tableHeader) => {
          return (
            <div
              className="grid-header-item"
              title={tableHeader.label}
              key={shortid.generate()}
              style={
                tableHeader.property ? { width: `${itemWidth}%`} : {}
              }
            >
              {
                tableHeader.property ?
                  this.renderSortableTableHeader(tableHeader)
                :
                  tableHeader.label
              }
            </div>
          );
        })
      }
      </div>
    );
  }
  render() {
    let tableClasses = classnames('table', this.props.className);
    return (
      <div className="table-container sortable-sticky-grid">
        <div className={tableClasses}>
          {this.renderTableHeader()}
        </div>
        <div className="table-scroll">
          {this.renderTableBody(this.state.entities)}
        </div>
      </div>
    );
  }
}

SortableStickyGrid.propTypes = {
  ...SortableTable.propTypes,
  renderTableHeaders: PropTypes.func
};
