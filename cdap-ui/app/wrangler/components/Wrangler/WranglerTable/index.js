/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, { Component, PropTypes } from 'react';
import WranglerStore from 'wrangler/components/Wrangler/Store/WranglerStore';
import ColumnActionsDropdown from 'wrangler/components/Wrangler/ColumnActionsDropdown';
import orderBy from 'lodash/orderBy';
import classnames from 'classnames';
import {filterData} from 'wrangler/components/Wrangler/filter-helper';
import Histogram from 'wrangler/components/Wrangler/Histogram';

import * as Table from 'reactabular-table';
import * as Sticky from 'reactabular-sticky';
import * as resolve from 'table-resolver';
import * as Virtualized from 'reactabular-virtualized';
import * as resizable from 'reactabular-resizable';

import shortid from 'shortid';


export default class WranglerTable extends Component {
  constructor(props) {
    super(props);

    let storeState = WranglerStore.getState().wrangler;

    this.state = {
      columns: this.getColumns(storeState),
      data: storeState.data,
      histogram: storeState.histogram,
      columnTypes: storeState.columnTypes,
      errors: storeState.errors,
      sort: storeState.sort,
      sortAscending: storeState.sortAscending,
      filter: storeState.filter
    };

    this.tableHeader = null;
    this.tableBody = null;
    this.typesBody = null;
  }

  componentWillMount() {
    this.resizableHelper = resizable.helper({
      globalId: shortid.generate(),
      getId: ({ property}) => property
    });

    WranglerStore.subscribe(() => {
      let state = WranglerStore.getState().wrangler;

      this.setState({
        data: state.data,
        columns: this.resizableHelper.initialize(this.getColumns(state)),
        histogram: state.histogram,
        columnTypes: state.columnTypes,
        errors: state.errors,
        sort: state.sort,
        sortAscending: state.sortAscending,
        filter: state.filter
      });
    });

    // Patch the column definition with class names.
    this.setState({
      columns: this.resizableHelper.initialize(this.state.columns)
    });
  }

  componentDidMount() {
    // We have refs now. Force update to get those to Header/Body.
    this.forceUpdate();
  }

  componentWillUnmount() {
    this.resizableHelper.cleanup();
  }

  onColumnClick(column) {
    /**
     * The Virtualized.Body does not get re-rendered on column click.
     * Therefore the class manipulation has to be done manually for the
     * existing element
     **/

    let prevActiveColumn = this.props.activeSelection;
    let prevColumns = document.getElementsByClassName(`column-${prevActiveColumn}`);

    let columns = document.getElementsByClassName(`column-${column}`);

    for (let i = 0; i < columns.length; i++) {
      if (prevActiveColumn) {
        prevColumns[i].classList.remove('active');
      }
      columns[i].classList.add('active');
    }

    this.props.onColumnClick(column);
  }

  getColumns(state) {
    const resizableFormatter = resizable.column({
      onDrag: (width, { column }) => {
        this.resizableHelper.update({
          column,
          width
        });
      }
    });

    const errorCircle = <i className="fa fa-circle error"></i>;
    const errors = state.errors;

    let width = this.props.width;

    let colWidth = (width - 50)/state.headersList.length;
    colWidth = colWidth < 150 ? 150 : colWidth;

    let columns = state.headersList.map((header) => {
      return {
        property: header,
        header: {
          label: header,
          transforms: [() => ({
            className: this.props.activeSelection === header ? 'top-header active' : '',
          })],
          formatters: [(head, extraParameters) => resizableFormatter(
            (<div style={{ display: 'inline' }}>
              <span
                className="header-text"
                onClick={this.onColumnClick.bind(this, head)}
              >
                {head}
              </span>

              <span className="pull-right">
                {errors[head] && errors[head].count ? errorCircle : null}
                <ColumnActionsDropdown column={head} />
              </span>
              </div>),
            extraParameters
          )]
        },
        cell: {
          transforms: [(cell, extra) => ({
            className: classnames(`column-${header}`, {
              'active': this.props.activeSelection === header,
              'column-type-cell': extra.rowIndex === -1
            }),
          })],
          formatters: [
            (value, extra) => {
              let isHistogram = extra.rowIndex === -2;
              return isHistogram ? (
                <Histogram
                  data={state.histogram[extra.property].data}
                  labels={state.histogram[extra.property].labels}
                />
              ) : (
                <span style={{width: '100%', display: 'inline-block'}}>
                  <span className="content">{value}</span>
                  {
                    errors[header] && errors[header][extra.rowData._index] ?
                      <span className="pull-right">{errorCircle}</span> : null
                  }
                </span>
              );
            }
          ]
        },
        width: colWidth
      };
    });

    columns.unshift({
      property: '_index',
      header: {
        label: '#',
        formatters: [resizableFormatter]
      },
      cell: {
        formatters: [
          (value, extra) => {
            let emptyArr = [-1, -2];

            let isEmpty = emptyArr.indexOf(extra.rowIndex) !== -1;
            return isEmpty ? '' : value + 1;
          }
        ]
      },
      props: {
        className: 'index-column text-center'
      },
      width: 50
    });


    return columns;
  }

  getClassName(column, i) {
    return `column-${this.id}-${i}`;
  }

  render() {
    const originalData = this.state.data;
    let data = originalData;

    let height = this.props.height - 41; // minus height of header
    let width = this.props.width;

    if (!height || !width) {
      return null;
    }

    if (this.state.sort) {
      let sortOrder = this.state.sortAscending ? 'asc' : 'desc';
      data = orderBy(originalData, [this.state.sort], [sortOrder]);
    }

    if (this.state.filter) {
      const filter = this.state.filter;
      data = filterData(data, filter.filterFunction, filter.column, filter.filterBy, filter.filterIgnoreCase);
    }

    let rows = resolve.resolve({
      columns: this.state.columns,
      method: resolve.index
    })(data);

    if (this.props.showHistogram) {
      let histogramRow = Object.assign({}, this.state.histogram, {_index: -2});
      rows.unshift(histogramRow);
    }

    let typeRow = Object.assign({}, this.state.columnTypes, {_index: -1});
    rows.unshift(typeRow);

    return (
      <Table.Provider
        className="table table-bordered"
        columns={this.state.columns}
        style={{ width: 'auto' }}
        components={{
          body: {
            wrapper: Virtualized.BodyWrapper,
            row: Virtualized.BodyRow
          }
        }}
      >
        <Sticky.Header
          ref={tableHeader => {
            this.tableHeader = tableHeader && tableHeader.getRef();
          }}
          tableBody={this.tableBody}
        />

        <Virtualized.Body
          rows={rows}
          rowKey="_index"
          height={height}
          ref={tableBody => {
            this.tableBody = tableBody && tableBody.getRef();
          }}
          tableHeader={this.tableHeader}
        />

      </Table.Provider>
    );
  }
}

WranglerTable.propTypes = {
  onColumnClick: PropTypes.func,
  activeSelection: PropTypes.string,
  showHistogram: PropTypes.bool,
  height: PropTypes.number,
  width: PropTypes.number
};
