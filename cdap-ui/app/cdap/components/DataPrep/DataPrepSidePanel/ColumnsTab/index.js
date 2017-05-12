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

import React, { Component } from 'react';
import UncontrolledPopover from 'components/UncontrolledComponents/Popover';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import MyDataPrepApi from 'api/dataprep';
import shortid from 'shortid';
import ColumnsTabRow from 'components/DataPrep/DataPrepSidePanel/ColumnsTab/ColumnsTabRow';
import ColumnsTabDetail from 'components/DataPrep/DataPrepSidePanel/ColumnsTab/ColumnsTabDetail';
import {objectQuery} from 'services/helpers';
import NamespaceStore from 'services/NamespaceStore';
import isEqual from 'lodash/isEqual';
import {directiveRequestBodyCreator} from 'components/DataPrep/helper';
import findIndex from 'lodash/findIndex';
import difference from 'lodash/difference';
import T from 'i18n-react';
import ColumnActions from 'components/DataPrep/Directives/ColumnActions';
const PREFIX = 'features.DataPrep.DataPrepSidePanel.ColumnsTab';

require('./ColumnsTab.scss');

export default class ColumnsTab extends Component {
  constructor(props) {
    super(props);

    this.state = {
      columns: {},
      headers: [],
      selectedHeaders: DataPrepStore.getState().dataprep.selectedHeaders,
      workspaceId: DataPrepStore.getState().dataprep.workspaceId,
      loading: true,
      error: null,
      searchText: '',
      detailRows: []
    };

    this.handleChangeSearch = this.handleChangeSearch.bind(this);
    this.clearSearch = this.clearSearch.bind(this);
    this.renderDropdown = this.renderDropdown.bind(this);
    this.clearAllColumns = this.clearAllColumns.bind(this);
    this.selectAllColumns = this.selectAllColumns.bind(this);
    this.setSelect = this.setSelect.bind(this);

    this.sub = DataPrepStore.subscribe(() => {
      let newState = DataPrepStore.getState().dataprep;
      let headers = newState.headers;
      let stateHeaders = this.state.headers.map(head => head.name);
      if (!isEqual(this.state.selectedHeaders, newState.selectedHeaders)) {
        this.setState({
          selectedHeaders: newState.selectedHeaders
        });
      }
      if (difference(stateHeaders, headers).length) {
        this.setState({
          loading: true
        });
        this.getSummary();
      }
    });

    this.getSummary();
  }

  componentWillUnmount() {
    this.sub();
  }

  clearAllColumns() {
    DataPrepStore.dispatch({
      type: DataPrepActions.setSelectedHeaders,
      payload: {
        selectedHeaders: []
      }
    });
  }

  selectAllColumns() {
    DataPrepStore.dispatch({
      type: DataPrepActions.setSelectedHeaders,
      payload: {
        selectedHeaders: DataPrepStore.getState().dataprep.headers
      }
    });
  }

  setSelect(columnName, selectStatus) {
    let currentSelectedHeaders = this.state.selectedHeaders.slice();
    if (selectStatus) {
      currentSelectedHeaders.push(columnName);
    } else {
      currentSelectedHeaders.splice(currentSelectedHeaders.indexOf(columnName), 1);
    }
    DataPrepStore.dispatch({
      type: DataPrepActions.setSelectedHeaders,
      payload: {
        selectedHeaders: currentSelectedHeaders
      }
    });
  }

  getSummary() {
    let state = DataPrepStore.getState().dataprep;
    if (!state.workspaceId) { return; }
    let namespace = NamespaceStore.getState().selectedNamespace;

    let params = {
      namespace,
      workspaceId: state.workspaceId
    };

    let requestBody = directiveRequestBodyCreator(state.directives);

    MyDataPrepApi.summary(params, requestBody)
      .subscribe((res) => {
        let columns = {};

        state.headers.forEach((head) => {
          columns[head] = {
            general: objectQuery(res, 'values', 'statistics', head, 'general'),
            types: objectQuery(res, 'values', 'statistics', head, 'types'),
            isValid: objectQuery(res, 'values', 'validation', head, 'valid')
          };
        });

        this.setState({
          columns,
          loading: false,
          headers: state.headers.map((res) => {
            let obj = {
              name: res,
              uniqueId: shortid.generate()
            };
            return obj;
          })
        });
      }, (err) => {
        console.log('error fetching summary', err);
        this.setState({
          loading: false,
          error: err.message
        });
      });
  }

  handleChangeSearch(e) {
    this.setState({searchText: e.target.value});
  }

  clearSearch() {
    this.setState({searchText: ''});
  }

  renderDropdown() {
    const tetherOption = {
      attachment: 'top left',
      targetAttachment: 'bottom right',
      targetOffset: '-5px 5px'
    };
    let element = document.getElementById('app-container');
    if (this.singleWorkspaceMode) {
      element = document.getElementsByClassName('wrangler-modal')[0];
    }
    // FIXME: Should this be a UncontrolledDropdown instead? One less component?
    return (
      <UncontrolledPopover
        tetherOption={tetherOption}
        documentElement={element}
      >
        <div
          className="toggle-all-option"
          onClick={this.clearAllColumns}
        >
          {T.translate(`${PREFIX}.toggle.clearAll`)}
        </div>
        <div
          className="toggle-all-option"
          onClick={this.selectAllColumns}
        >
          {T.translate(`${PREFIX}.toggle.selectAll`)}
        </div>
      </UncontrolledPopover>
    );
  }

  showDetail(rowId) {
    let index = findIndex(this.state.headers, (header => header.uniqueId === rowId));
    let match = this.state.headers[index];
    let modifiedHeaders = this.state.headers.slice(0);
    if (match.expanded) {
      match.expanded = false;
      modifiedHeaders = [
        ...modifiedHeaders.slice(0, index + 1),
        ...modifiedHeaders.slice(index + 2)
      ];
    } else {
      match.expanded = true;
      modifiedHeaders = [
        ...modifiedHeaders.slice(0, index + 1),
        Object.assign({}, modifiedHeaders[index], {
          isDetail: true,
          uniqueId: shortid.generate()
        }),
        ...modifiedHeaders.slice(index + 1)
      ];
    }
    this.setState({
      headers: modifiedHeaders
    });
  }

  render() {
    if (this.state.loading) {
      return (
        <div className="columns-tab text-xs-center">
          <span className="fa fa-spin fa-spinner" />
        </div>
      );
    }

    let index = -1;
    let displayHeaders = this.state.headers.map(header => {
      if (!header.isDetail) {
        index += 1;
        return Object.assign({}, header, {index});
      }
      return header;
    });

    if (this.state.searchText.length > 0) {
      displayHeaders = displayHeaders.filter((head) => {
        let headerLower = head.name.toLowerCase();
        let search = this.state.searchText.toLowerCase();

        return headerLower.indexOf(search) !== -1;
      });
    }

    return (
      <div className="columns-tab">
        <div className="columns-tab-heading">
          <div className="search-box">
            <input
              type="text"
              className="form-control"
              placeholder={T.translate(`${PREFIX}.searchPlaceholder`)}
              value={this.state.searchText}
              onChange={this.handleChangeSearch}
            />

            {
              this.state.searchText.length === 0 ?
                (<span className="fa fa-search" />)
              :
                (
                  <span
                    className="fa fa-times-circle"
                    onClick={this.clearSearch}
                  />
                )
            }
          </div>
          <ColumnActions />
        </div>
        <div className="columns-list">
          <table className="table table-sm table-responsive table-hover">
            <thead>
              <tr>
                <th>
                  { this.renderDropdown() }
                </th>
                <th>
                  #
                </th>
                <th>
                  {T.translate(`${PREFIX}.Header.name`)}
                </th>
                <th>
                  {T.translate(`${PREFIX}.Header.completion`)}
                </th>
              </tr>
            </thead>
            <tbody>
              {
                displayHeaders.map((head) => {
                  if (head.isDetail) {
                    return (
                      <ColumnsTabDetail
                        key={head.uniqueId}
                        columnInfo={this.state.columns[head.name]}
                      />
                    );
                  }
                  return (
                    <ColumnsTabRow
                      rowInfo={this.state.columns[head.name]}
                      onShowDetails={this.showDetail.bind(this, head.uniqueId)}
                      columnName={head.name}
                      index={head.index}
                      key={head.uniqueId}
                      selected={this.state.selectedHeaders.indexOf(head.name) !== -1}
                      setSelect={this.setSelect}
                    />
                  );
                })
              }
            </tbody>
          </table>
        </div>
      </div>
    );
  }
}
