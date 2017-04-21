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
import {Popover, PopoverContent} from 'reactstrap';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import MyDataPrepApi from 'api/dataprep';
import shortid from 'shortid';
import classnames from 'classnames';
import ColumnsTabRow from 'components/DataPrep/DataPrepSidePanel/ColumnsTab/ColumnsTabRow';
import {objectQuery} from 'services/helpers';
import NamespaceStore from 'services/NamespaceStore';
import isEqual from 'lodash/isEqual';
import {directiveRequestBodyCreator} from 'components/DataPrep/helper';
import Mousetrap from 'mousetrap';
import {isDescendant} from 'services/helpers';
import Rx from 'rx';
import T from 'i18n-react';

const PREFIX = 'features.DataPrep.SidePanel.ColumnsTab';

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
      dropdownOpen: false
    };

    this.handleChangeSearch = this.handleChangeSearch.bind(this);
    this.clearSearch = this.clearSearch.bind(this);
    this.toggleDropdown = this.toggleDropdown.bind(this);
    this.clearAllColumns = this.clearAllColumns.bind(this);
    this.selectAllColumns = this.selectAllColumns.bind(this);
    this.setSelect = this.setSelect.bind(this);

    this.sub = DataPrepStore.subscribe(() => {
      let newState = DataPrepStore.getState().dataprep;
      if (!isEqual(this.state.selectedHeaders, newState.selectedHeaders)) {
        this.setState({
          selectedHeaders: newState.selectedHeaders
        });
      }
      this.getSummary();
    });

    this.getSummary();
  }

  componentWillUnmount() {
    this.sub();
  }

  toggleDropdown() {
    let newState = !this.state.dropdownOpen;
    this.setState({
      dropdownOpen: newState
    });

    if (newState) {
      let element = document.getElementById('app-container');
      if (this.singleWorkspaceMode) {
        element = document.getElementsByClassName('wrangler-modal')[0];
      }
      this.documentClick$ = Rx.Observable.fromEvent(element, 'click')
      .subscribe((e) => {
        if (isDescendant(this.popover, e.target) || !this.state.dropdownOpen) {
          return;
        }

        this.toggleDropdown();
      });
      Mousetrap.bind('esc', this.toggleDropdown);
    } else {
      if (this.documentClick$) {
        this.documentClick$.dispose();
      }
      Mousetrap.unbind('esc');
    }
  }

  clearAllColumns() {
    DataPrepStore.dispatch({
      type: DataPrepActions.setSelectedHeaders,
      payload: {
        selectedHeaders: []
      }
    });
    this.toggleDropdown();
  }

  selectAllColumns() {
    DataPrepStore.dispatch({
      type: DataPrepActions.setSelectedHeaders,
      payload: {
        selectedHeaders: DataPrepStore.getState().dataprep.headers
      }
    });
    this.toggleDropdown();
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

    return (
      <Popover
        placement="bottom right"
        isOpen={this.state.dropdownOpen}
        target="toggle-all-dropdown"
        className="dataprep-toggle-all-dropdown"
        tether={tetherOption}
      >
        <PopoverContent>
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
        </PopoverContent>
      </Popover>
    );
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
        <div className="columns-tab-heading">
          <span
            className={classnames('fa fa-caret-square-o-down', {
              'expanded': this.state.dropdownOpen
            })}
            id="toggle-all-dropdown"
            onClick={this.toggleDropdown}
            ref={(ref) => this.popover = ref}
          />
          { this.renderDropdown() }
          <span className="search-box">
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
          </span>
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
                  selected={this.state.selectedHeaders.indexOf(head.name) !== -1}
                  setSelect={this.setSelect}
                />
              );
            })
          }
        </div>
      </div>
    );
  }
}
