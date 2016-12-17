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
import classnames from 'classnames';
import NamespaceStore from 'services/NamespaceStore';
import myExploreApi from 'api/explore';
import {Modal, ModalHeader, ModalBody, Tooltip} from 'reactstrap';
import shortid from 'shortid';
import Papa from 'papaparse';
import T from 'i18n-react';

require('./TableItem.less');

export default class TableItem extends Component {
  constructor(props) {
    super(props);

    this.state = {
      isModalOpen: false,
      tooltipOpen: false,
      loading: false,
      columns: [],
      preview: [],
      fieldSelected: '',
      queryHandle: ''
    };

    this.onTableClick = this.onTableClick.bind(this);
    this.toggleModal = this.toggleModal.bind(this);
    this.onWrangleClick = this.onWrangleClick.bind(this);
    this.handleData = this.handleData.bind(this);
    this.setDelimiter = this.setDelimiter.bind(this);
    this.tooltipToggle = this.tooltipToggle.bind(this);
  }

  setDelimiter(e) {
    this.setState({delimiter: e.target.value});
  }

  pollQueryStatus(queryHandle) {
    this.queryStatusPoll$ = myExploreApi.pollQueryStatus({queryHandle})
      .subscribe((res) => {
        if (res.status === 'FINISHED') {
          this.fetchQueryResults(queryHandle);
        }
      });
  }

  fetchQueryResults(queryHandle) {
    this.queryStatusPoll$.dispose();
    this.queryStatusPoll$.dispose(); // check?

    myExploreApi.getQuerySchema({queryHandle})
      .combineLatest(myExploreApi.getQueryPreview({queryHandle}))
      .subscribe((res) => {
        let columns = res[0].map((header) => {
          let name;
          if (header.name.indexOf('.') !== -1) {
            name = header.name.split('.')[1];
          }

          return {
            raw: header.name,
            displayName: name
          };
        });
        let preview = res[1].map((row) => row.columns);

        this.setState({
          columns,
          preview,
          queryHandle,
          loading: false
        });

      });
  }

  onTableClick() {
    this.setState({
      isModalOpen: true,
      loading: true
    });

    const table = this.props.table;

    const query = `SELECT * FROM ${table.type}_${table.name} LIMIT 500`;
    const namespace = NamespaceStore.getState().selectedNamespace;

    myExploreApi
      .submitQuery({namespace}, {query})
      .subscribe((res) => {
        let queryHandle = res.handle;
        this.pollQueryStatus(queryHandle);
      });
  }

  toggleModal() {
    this.setState({isModalOpen: !this.state.isModalOpen});
  }

  tooltipToggle() {
    this.setState({tooltipOpen: !this.state.tooltipOpen});
  }

  onWrangleClick() {
    myExploreApi.download({queryHandle: this.state.queryHandle})
      .subscribe((res) => {
        let papaConfig = {
          header: true,
          skipEmptyLines: true,
          complete: this.handleData
        };

        Papa.parse(res, papaConfig);
      });
  }

  handleData(papa) {
    let formattedData = papa.data.map((row) => row[this.state.fieldSelected]).join('\n');
    this.setState({isModalOpen: false});

    this.props.wrangle(
      formattedData,
      this.state.delimiter,
      false,
      false
    );
  }

  renderModalContent() {
    const table = (
      <table className="table table-bordered">
        <thead>
          <tr>
            {this.state.columns.map((column) => <th key={column.raw}>{column.displayName}</th>)}
          </tr>
        </thead>

        <tbody>
          {
            this.state.preview.map((row) => {
              return (
                <tr key={shortid.generate()}>
                  {row.map((columnData) => <td key={shortid.generate()}>{columnData}</td>)}
                </tr>
              );
            })
          }
        </tbody>
      </table>
    );

    const noData = (
      <div>
        <h4 className="text-center">
          {T.translate('features.Wrangler.Explore.noData')}
        </h4>
      </div>
    );

    return (
      <div>
        <div className="table-container">
          {this.state.preview.length === 0 ? noData : table}
        </div>

        <div className="form-inline field-selection">
          <label className="control-label">
            {T.translate('features.Wrangler.Explore.chooseField')}
          </label>

          <select
            className="form-control"
            value={this.state.fieldSelected}
            onChange={e => this.setState({fieldSelected: e.target.value})}
          >
            {
              this.state.columns.map((column) => {
                return (
                  <option
                    value={column.raw}
                    key={column.raw}
                  >
                    {column.displayName}
                  </option>
                );
              })
            }
          </select>
        </div>

        <div className="parse-options">
          <form className="form-inline">
            <div className="delimiter">
              {/* delimiter */}
              <input
                type="text"
                className="form-control"
                placeholder={T.translate('features.Wrangler.InputScreen.Options.delimiter')}
                onChange={this.setDelimiter}
              />
            </div>
          </form>
        </div>

        <div className="wrangle-button text-center">
          <button
            className="btn btn-wrangler"
            onClick={this.onWrangleClick}
          >
            {T.translate('features.Wrangler.wrangleButton')}
          </button>
        </div>
      </div>
    );
  }

  render() {
    const loading = (
      <div>
        <h3 className="text-center">
          <span className="fa fa-spinner fa-spin" />
        </h3>
      </div>
    );

    const id = `explore-item-${this.props.table.name}`;

    return (
      <div
        className="explore-table-item text-center"
        onClick={this.onTableClick}
      >
        <div className="explore-table-item-icon">
          <span className={classnames('fa', {
            'icon-streams': this.props.table.type === 'stream',
            'icon-datasets': this.props.table.type === 'dataset'
          })} />
        </div>
        <div className="explore-table-item-name">
          <span id={id}>{this.props.table.name}</span>

          <Tooltip
            placement="top"
            isOpen={this.state.tooltipOpen}
            toggle={this.tooltipToggle}
            target={id}
            className="wrangler-tooltip"
            delay={0}
          >
            {T.translate(`commons.entity.${this.props.table.type}.singular`)}: {this.props.table.name}
          </Tooltip>
        </div>

        <Modal
          className="explore-table-modal"
          toggle={this.toggleModal}
          isOpen={this.state.isModalOpen}
          size="lg"
          zIndex="1070"
        >
          <ModalHeader>
            <span>{this.props.table.name}</span>

            <div
              className="close-section pull-right"
              onClick={this.toggleModal}
            >
              <span className="fa fa-times" />
            </div>
          </ModalHeader>
          <ModalBody>
            {
              this.state.loading ? loading : this.renderModalContent()
            }
          </ModalBody>
        </Modal>
      </div>
    );
  }
}

TableItem.propTypes = {
  table: PropTypes.shape({
    name: PropTypes.string,
    type: PropTypes.oneOf(['dataset', 'stream'])
  }),
  wrangle: PropTypes.func
};
