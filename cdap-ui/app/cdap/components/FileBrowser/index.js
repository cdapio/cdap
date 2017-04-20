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

import React, { Component, PropTypes } from 'react';
import MyDataPrepApi from 'api/dataprep';
import NamespaceStore from 'services/NamespaceStore';
import shortid from 'shortid';
import classnames from 'classnames';
import moment from 'moment';
import {Link} from 'react-router';
import FilePath from 'components/FileBrowser/FilePath';
import {convertBytesToHumanReadable, HUMANREADABLESTORAGE_NODECIMAL} from 'services/helpers';
import T from 'i18n-react';

require('./FileBrowser.scss');

const BASEPATH = '/Users';
const PREFIX = 'features.FileBrowser';

export default class FileBrowser extends Component {
  constructor(props) {
    super(props);

    this.state = {
      contents: [],
      path: '',
      statePath: this.props.pathname,
      error: null,
      loading: true,
      search: ''
    };

    this.handleSearch = this.handleSearch.bind(this);
  }

  componentWillReceiveProps(nextProps) {
    this.fetchDirectory(nextProps);
  }

  fetchDirectory(props) {
    let hdfsPath;

    if (this.props.noState) {
      hdfsPath = this.props.initialDirectoryPath;
      return;
    } else {
      hdfsPath = props.location.pathname.slice(props.pathname.length);
      hdfsPath = hdfsPath || this.props.initialDirectoryPath || BASEPATH;
    }

    if (hdfsPath === this.state.path) { return; }

    this.goToPath(hdfsPath);
  }

  goToPath(path) {
    this.setState({loading: true});

    let namespace = NamespaceStore.getState().selectedNamespace;

    MyDataPrepApi.explorer({
      namespace,
      path
    }).subscribe((res) => {
      this.setState({
        loading: false,
        contents: this.formatResponse(res.values),
        error: null,
        search: '',
        path
      });
    }, (err) => {
      this.setState({
        loading: false,
        error: err.response,
        search: '',
        path
      });
    });
  }

  formatResponse(contents) {
    return contents.map((content) => {
      content.uniqueId = shortid.generate();
      content['last-modified'] = moment(content['last-modified']).format('MM/DD/YY HH:mm');
      content.size = convertBytesToHumanReadable(content.size, HUMANREADABLESTORAGE_NODECIMAL);
      content.type = content.type === 'UNKNOWN' ? '--' : content.type;
      return content;
    });
  }

  handleSearch(e) {
    this.setState({search: e.target.value});
  }

  renderRow(row) {
    const directoryText = T.translate(`${PREFIX}.directory`);
    let directoryDisplay = row.directory ? directoryText : row.type;

    return (
      <div
        key={row.uniqueId}
        className="row content-row"
      >
        <div className="col-xs-3 name">
          <span
            className={classnames('type-icon fa fa-fw', {
              'folder-icon fa-folder-o': row.directory,
              'file-icon fa-file-o': !row.directory
            })}
          />
          <span title={row.name}>{row.name}</span>
        </div>
        <div className="col-xs-2">
          <span title={directoryDisplay}>
            {directoryDisplay}
          </span>
        </div>
        <div className="col-xs-1">
          <span title={row.size}>
            {row.size}
          </span>
        </div>
        <div className="col-xs-2">
          <span title={row['last-modified']}>
            {row['last-modified']}
          </span>
        </div>
        <div className="col-xs-1">
          <span title={row.owner}>
            {row.owner}
          </span>
        </div>
        <div className="col-xs-1">
          <span title={row.group}>
            {row.group}
          </span>
        </div>
        <div className="col-xs-2">
          <span title={row.permission}>
            {row.permission}
          </span>
        </div>
      </div>
    );
  }

  renderDirectory(content) {
    if (this.props.noState) {
      return (
        <div
          className="row-container"
          onClick={this.goToPath.bind(this, content.path)}
        >
          {this.renderRow(content)}
        </div>
      );
    }

    let linkPath = `${this.state.statePath}${content.path}`;

    return (
      <Link to={linkPath}>
        {this.renderRow(content)}
      </Link>
    );
  }

  renderContent() {
    if (this.state.loading) {
      // NEED TO REPLACE WITH ACTUAL LOADING ICON

      return (
        <div className="loading-container">
          <br />
          <h3 className="text-xs-center">
            <span className="fa fa-spin fa-spinner" />
          </h3>
        </div>
      );
    }

    if (this.state.error) {
      return (
        <div className="error-container">
          <br />
          <h4 className="text-xs-center text-danger">
            {this.state.error}
          </h4>
        </div>
      );
    }

    if (this.state.contents.length === 0) {
      return (
        <h5 className="text-xs-center">Empty</h5>
      );
    }

    let displayContent = this.state.contents;

    if (this.state.search.length > 0) {
      displayContent = this.state.contents.filter((content) => {
        let contentName = content.name.toLowerCase();
        let searchText = this.state.search.toLowerCase();

        return contentName.indexOf(searchText) !== -1;
      });
    }

    return (
      <div className="directory-content-table">
        <div className="content-header row">
          <div className="col-xs-3">
            {T.translate(`${PREFIX}.Table.name`)}
          </div>

          <div className="col-xs-2">
            {T.translate(`${PREFIX}.Table.type`)}
          </div>

          <div className="col-xs-1">
            {T.translate(`${PREFIX}.Table.size`)}
          </div>

          <div className="col-xs-2">
            {T.translate(`${PREFIX}.Table.lastModified`)}
          </div>

          <div className="col-xs-1">
            {T.translate(`${PREFIX}.Table.owner`)}
          </div>

          <div className="col-xs-1">
            {T.translate(`${PREFIX}.Table.group`)}
          </div>

          <div className="col-xs-2">
            {T.translate(`${PREFIX}.Table.permission`)}
          </div>
        </div>

        <div className="content-body clearfix">
          {
            displayContent.map((content) => {
              return content.directory ? this.renderDirectory(content) : this.renderRow(content);
            })
          }
        </div>

      </div>
    );
  }

  render() {
    return (
      <div className="file-browser-container">
        <div className="top-panel">
          <div className="title">
            <h5>
              {T.translate(`${PREFIX}.TopPanel.selectData`)}
            </h5>
          </div>
        </div>

        <div className="sub-panel clearfix">
          <div className="path-container float-xs-left">
            <FilePath
              baseStatePath={this.state.statePath}
              fullpath={this.state.path}
            />
          </div>

          <div className="float-xs-right">
            <div className="info">
              <span>
                {T.translate(`${PREFIX}.TopPanel.directoryMetrics`, {count: this.state.contents.length})}
              </span>
            </div>

            <div className="search-container">
              <input
                type="text"
                className="form-control"
                placeholder={T.translate(`${PREFIX}.TopPanel.searchPlaceholder`)}
                value={this.state.search}
                onChange={this.handleSearch}
              />
            </div>
          </div>
        </div>

        {this.renderContent()}
      </div>
    );
  }
}

FileBrowser.propTypes = {
  location: PropTypes.shape({
    pathname: PropTypes.string
  }),
  pathname: PropTypes.string,
  initialDirectoryPath: PropTypes.string,
  noState: PropTypes.bool
};


