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
import DataPrepStore from 'components/DataPrep/store';
import MyDataPrepApi from 'api/dataprep';
import shortid from 'shortid';
import ColumnsTabRow from 'components/DataPrep/DataPrepSidePanel/ColumnsTab/ColumnsTabRow';
import {objectQuery} from 'services/helpers';
import NamespaceStore from 'services/NamespaceStore';

export default class ColumnsTab extends Component {
  constructor(props) {
    super(props);

    this.state = {
      columns: {},
      headers: [],
      loading: true,
      error: null,
      searchText: ''
    };

    this.handleChangeSearch = this.handleChangeSearch.bind(this);
    this.clearSearch = this.clearSearch.bind(this);

    this.sub = DataPrepStore.subscribe(this.getSummary.bind(this));

    this.getSummary();
  }

  componentWillUnmount() {
    this.sub();
  }

  getSummary() {
    let state = DataPrepStore.getState().dataprep;
    if (!state.workspaceId) { return; }
    let namespace = NamespaceStore.getState().selectedNamespace;

    let params = {
      namespace,
      workspaceId: state.workspaceId,
      limit: 100,
      directive: state.directives
    };

    MyDataPrepApi.summary(params)
      .subscribe((res) => {
        let columns = {};

        state.headers.forEach((head) => {
          columns[head] = {
            general: objectQuery(res, 'value', 'statistics', head, 'general'),
            types: objectQuery(res, 'value', 'statistics', head, 'types'),
            isValid: objectQuery(res, 'value', 'validation', head, 'valid')
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

  render() {
    if (this.state.loading) {
      return (
        <div className="columns-tab text-xs-center">
          <span className="fa fa-spin fa-spinner" />
        </div>
      );
    }

    let displayHeaders = this.state.headers;

    if (this.state.searchText.length > 0) {
      displayHeaders = displayHeaders.filter((head) => {
        let headerLower = head.name.toLowerCase();
        let search = this.state.searchText.toLowerCase();

        return headerLower.indexOf(search) !== -1;
      });
    }

    return (
      <div className="columns-tab">
        <div className="search-box">
          <input
            type="text"
            className="form-control"
            placeholder="search"
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
        <div className="columns-list">
          {
            displayHeaders.map((head, index) => {
              return (
                <ColumnsTabRow
                  rowInfo={this.state.columns[head.name]}
                  columnName={head.name}
                  index={index}
                  key={head.uniqueId}
                />
              );
            })
          }
        </div>
      </div>
    );
  }
}
