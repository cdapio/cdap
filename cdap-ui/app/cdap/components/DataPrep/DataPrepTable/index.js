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
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import shortid from 'shortid';
import ee from 'event-emitter';
import classnames from 'classnames';
import ColumnActionsDropdown from 'components/DataPrep/ColumnActionsDropdown';
require('./DataPrepTable.scss');
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import TextboxOnValium from 'components/TextboxOnValium';
import WarningContainer from 'components/WarningContainer';
import ColumnHighlighter from 'components/DataPrep/ColumnHighlighter';
import isNil from 'lodash/isNil';
import T from 'i18n-react';
import DataQuality from 'components/DataPrep/DataPrepTable/DataQuality';

export default class DataPrepTable extends Component {
  constructor(props) {
    super(props);

    let storeState = DataPrepStore.getState();

    this.state = {
      headers: storeState.dataprep.headers.map(header => ({name: header, edit: false})),
      data: storeState.dataprep.data,
      loading: !storeState.dataprep.initialized,
      directivesLength: storeState.dataprep.directives.length,
      workspaceId: storeState.dataprep.workspaceId,
      columns: storeState.columnsInformation.columns,
      selectedHeaders: storeState.dataprep.selectedHeaders
    };

    this.eventEmitter = ee(ee);
    this.openUploadData = this.openUploadData.bind(this);
    this.openCreateWorkspaceModal = this.openCreateWorkspaceModal.bind(this);
    this.switchToEditColumnName = this.switchToEditColumnName.bind(this);
    this.toggleColumnSelect = this.toggleColumnSelect.bind(this);
    this.columnIsSelected = this.columnIsSelected.bind(this);
    this.columnDropdownOpened = this.columnDropdownOpened.bind(this);

    this.sub = DataPrepStore.subscribe(() => {
      let state = DataPrepStore.getState();
      this.setState({
        data: state.dataprep.data.map(d => Object.assign({}, d, {uniqueId: shortid.generate()})),
        headers: state.dataprep.headers.map(header => ({name: header, edit: false})),
        loading: !state.dataprep.initialized,
        directivesLength: state.dataprep.directives.length,
        workspaceId: state.dataprep.workspaceId,
        selectedHeaders: state.dataprep.selectedHeaders,
        columns: state.columnsInformation.columns
      });
    });
  }

  componentWillUnmount() {
    this.sub();
  }

  toggleColumnSelect(columnName) {
    let currentSelectedHeaders = this.state.selectedHeaders.slice();
    if (!this.columnIsSelected(columnName)) {
      currentSelectedHeaders.push(columnName);
      let elem = document.querySelector(`#columns-tab-row-${columnName} .row-header`);
      if (elem) {
        elem.scrollIntoView();
      }
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

  columnDropdownOpened(columnDropdown, openState) {
    if (openState) {
      this.setState({
        columnDropdownOpen: columnDropdown
      });
    } else {
      this.setState({
        columnDropdownOpen: null
      });
    }
  }

  columnIsSelected(columnName) {
    return this.state.selectedHeaders.indexOf(columnName) !== -1;
  }

  openCreateWorkspaceModal() {
    this.eventEmitter.emit('DATAPREP_CREATE_WORKSPACE');
  }

  openUploadData() {
    this.eventEmitter.emit('DATAPREP_OPEN_UPLOAD');
  }

  switchToEditColumnName(head) {
    let newHeaders = this.state.headers.map(header => {
      if (header.name === head.name) {
        return Object.assign({}, header, {
          edit: !header.edit,
          showWarning: false,
        });
      }
      return {
        name: header.name,
        edit: false
      };
    });
    this.setState({
      headers: newHeaders
    });
  }

  showWarningMessage(index, currentValue) {
    let showWarning = this.state.headers
      .filter(header => header.name === currentValue);
    let headers = this.state.headers;
    let matchedHeader = headers[index];
    if (!showWarning.length || headers[index].name === currentValue) {
      if (matchedHeader.showWarning) {
        matchedHeader.showWarning = false;
        delete matchedHeader.editedColumnName;
        this.setState({
          headers: [
            ...headers.slice(0, index),
            matchedHeader,
            ...headers.slice(index + 1)
          ]
        });
      }
      return;
    }
    matchedHeader.showWarning = true;
    matchedHeader.editedColumnName = currentValue;
    this.setState({
      headers: [
        ...headers.slice(0, index),
        matchedHeader,
        ...headers.slice(index + 1)
      ]
    });
    return true;
  }
  handleSaveEditedColumnName(index, changedValue, noChange) {
    let headers = this.state.headers;
    let matchedHeader = headers[index];
    if (!noChange) {
      this.applyDirective(`rename ${matchedHeader.name} ${changedValue}`);
      matchedHeader.name = changedValue;
    }
    matchedHeader.edit = false;
    matchedHeader.showWarning = false;
    delete matchedHeader.editedColumnName;
    this.setState({
      headers: [
        ...headers.slice(0, index),
        matchedHeader,
        ...headers.slice(index + 1)
      ]
    });
  }
  applyDirective(directive) {
    execute([directive])
      .subscribe(
        () => {},
        (err) => {
          DataPrepStore.dispatch({
            type: DataPrepActions.setError,
            payload: {
              message: err.message || err.response.message
            }
          });
        }
      );
  }
  renderDataprepTable() {
    let headers = this.state.headers;
    let data = this.state.data;

    return (
      <table className="table table-bordered">
        <thead className="thead-inverse">
          {
            headers.map((head, index) => {
              return (
                <th
                  id={`column-${head.name}`}
                  className={classnames({
                    'selected': this.columnIsSelected(head.name),
                    'dropdownOpened': this.state.columnDropdownOpen === head.name
                  })}
                  key={head.name}
                >
                  <DataQuality columnInfo={this.state.columns[head.name]} />
                  <div
                    className="clearfix column-wrapper"
                  >
                    <span className="directives-dropdown-button">
                      <ColumnActionsDropdown
                        column={head.name}
                        dropdownOpened={this.columnDropdownOpened}
                      />
                    </span>
                    {
                      !head.edit ?
                        <span
                          className="header-text"
                          onClick={this.switchToEditColumnName.bind(this, head)}
                        >
                          <span>
                            {head.name}
                          </span>
                        </span>
                      :
                        <div className="warning-container-wrapper float-xs-left">
                          <TextboxOnValium
                            onChange={this.handleSaveEditedColumnName.bind(this, index)}
                            value={head.name}
                            onWarning={this.showWarningMessage.bind(this, index)}
                            allowSpace={false}
                          />
                          {
                            head.showWarning ?
                              <WarningContainer
                                message={T.translate('features.DataPrep.DataPrepTable.columnEditWarningMessage')}
                              >
                                <div className="warning-btns-container">
                                  <div
                                    className="btn btn-primary"
                                    onClick={this.handleSaveEditedColumnName.bind(this, index, head.editedColumnName, false)}
                                  >
                                    {T.translate('features.DataPrep.Directives.apply')}
                                  </div>
                                  <div
                                    className="btn"
                                    onClick={this.handleSaveEditedColumnName.bind(this, index, head.name, true)}
                                  >
                                    {T.translate('features.DataPrep.Directives.cancel')}
                                  </div>
                                </div>
                              </WarningContainer>
                            :
                              null
                          }
                        </div>
                    }
                    <span
                      onClick={this.toggleColumnSelect.bind(this, head.name)}
                      className={classnames('float-xs-right fa column-header-checkbox', {
                        'fa-square-o': !this.columnIsSelected(head.name),
                        'fa-check-square': this.columnIsSelected(head.name)
                      })}
                    />
                  </div>
                </th>
              );
            })
          }
        </thead>
        <tbody>
          {
            data.map((row) => {
              return (
                <tr key={row.uniqueId}>
                  {headers.map((head, i) => {
                    return <td key={i}><div>{row[head.name]}</div></td>;
                  })}
                </tr>
              );
            })
          }
        </tbody>
      </table>
    );
  }
  render() {
    if (this.state.loading) {
      return (
        <div className="dataprep-table empty">
          <h4 className="text-xs-center">
            <span className="fa fa-spin fa-spinner" />
          </h4>
        </div>
      );
    }

    let headers = this.state.headers;
    let data = this.state.data;

    if (!this.state.workspaceId) {
      return (
        <div className="dataprep-table empty">
          <div>
            <h5 className="text-xs-center">Please select or upload a file to wrangle data</h5>
          </div>
        </div>
      );
    }

    // FIXME: Not sure if this is possible now.
    if (data.length === 0 || headers.length === 0) {
      return (
        <div className="dataprep-table empty">
          {
            this.state.directivesLength === 0 ?
              (
                <div>
                  <h5 className="text-xs-center">
                    {T.translate('features.DataPrep.DataPrepTable.emptyWorkspace')}
                  </h5>
                </div>
              ) :
              (
                <div>
                  <h5 className="text-xs-center">
                    {T.translate('features.DataPrep.DataPrepTable.noData')}
                  </h5>
                </div>
              )
          }
        </div>
      );
    }
    let {highlightColumns} = DataPrepStore.getState().dataprep;
    return (
      <div className={
          classnames("dataprep-table", {
            'column-highlighted': !isNil(highlightColumns.directive)
          })
        } id="dataprep-table-id">
        <ColumnHighlighter />
        {
          isNil(highlightColumns.directive) ?
            this.renderDataprepTable()
          :
            null
        }
      </div>
    );
  }
}
