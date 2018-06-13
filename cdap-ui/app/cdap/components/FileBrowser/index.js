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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import MyDataPrepApi from 'api/dataprep';
import NamespaceStore from 'services/NamespaceStore';
import uuidV4 from 'uuid/v4';
import classnames from 'classnames';
import moment from 'moment';
import {Link} from 'react-router-dom';
import FilePath from 'components/FileBrowser/FilePath';
import {convertBytesToHumanReadable, HUMANREADABLESTORAGE_NODECIMAL, preventPropagation as preventPropagationService, objectQuery} from 'services/helpers';
import T from 'i18n-react';
import orderBy from 'lodash/orderBy';
import IconSVG from 'components/IconSVG';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import isEmpty from 'lodash/isEmpty';
import DataPrepStore from 'components/DataPrep/store';
import lastIndexOf from 'lodash/lastIndexOf';
import isNil from 'lodash/isNil';

require('./FileBrowser.scss');

const BASEPATH = '/';
const PREFIX = 'features.FileBrowser';

export default class FileBrowser extends Component {

  state = {
    contents: [],
    path: '',
    statePath: objectQuery(this.props, 'match', 'url') || '',
    error: null,
    loading: true,
    search: '',
    sort: 'name',
    sortOrder: 'asc',
    searchFocus: true
  };

  static defaultProps = {
    enableRouting: true,
    scope: false,
    browserTitle: T.translate(`${PREFIX}.TopPanel.selectData`)
  };

  static propTypes = {
    location: PropTypes.object,
    match: PropTypes.object,
    initialDirectoryPath: PropTypes.string,
    noState: PropTypes.bool,
    toggle: PropTypes.func.isRequired,
    enableRouting: PropTypes.bool,
    onWorkspaceCreate: PropTypes.func,
    scope: PropTypes.oneOfType([PropTypes.bool, PropTypes.string]),
    browserTitle: PropTypes.string
  };

  componentWillMount() {
    if (!this.props.enableRouting) {
      let path = this.getFilePath();
      this.dataprepSubscription = DataPrepStore.subscribe(() => {
        let path = this.getFilePath();
        if (!isNil(path) && path !== this.state.path) {
          this.goToPath(path);
        }
      });
      this.goToPath(path);
    } else {
      this.fetchDirectory(this.props);
    }
  }

  componentWillUnmount() {
    if (this.dataprepSubscription) {
      this.dataprepSubscription();
    }
  }

  componentWillReceiveProps(nextProps) {
    if (!this.props.enableRouting) {
      // When routing is disabled location, match are not entirely right.
      let path = this.getFilePath();
      if (!isNil(path) && path !== this.state.path) {
        this.goToPath(path);
      }
    } else {
      this.fetchDirectory(nextProps);
    }
  }

  getFilePath() {
    let {workspaceInfo} = DataPrepStore.getState().dataprep;
    let filePath = objectQuery(workspaceInfo, 'properties', 'path');
    filePath = !isEmpty(filePath) ? filePath.slice(0, lastIndexOf(filePath, '/') + 1) : this.state.path;
    if (isEmpty(filePath)) {
      filePath = BASEPATH;
    }
    return filePath;
  }

  preventPropagation = (e) => {
    if (this.props.enableRouting) {
      return;
    }
    preventPropagationService(e);

  }

  fetchDirectory(props) {
    let hdfsPath;

    if (this.props.noState) {
      hdfsPath = this.props.initialDirectoryPath;
      return;
    } else {
      hdfsPath = props.location.pathname.slice(props.match.url.length);
      hdfsPath = hdfsPath || this.props.initialDirectoryPath || BASEPATH;
    }

    if (hdfsPath === this.state.path) { return; }

    this.goToPath(hdfsPath);
  }

  goToPath = (path) => {
    this.setState({
      loading: true,
      path
    });

    let namespace = NamespaceStore.getState().selectedNamespace;

    MyDataPrepApi.explorer({
      namespace,
      path,
      hidden: true
    }).subscribe((res) => {
      this.setState({
        loading: false,
        contents: this.formatResponse(res.values),
        error: null,
        search: ''
      });
    }, (err) => {
      console.log('err', err);

      this.setState({
        loading: false,
        error: err.response.message || err.response,
        search: ''
      });
    });
  }

  formatResponse(contents) {
    return contents.map((content) => {
      content.uniqueId = uuidV4();
      content['last-modified'] = moment(content['last-modified']).format('MM/DD/YY HH:mm');
      content.displaySize = convertBytesToHumanReadable(content.size, HUMANREADABLESTORAGE_NODECIMAL, true);

      if (content.directory) {
        content.type = T.translate(`${PREFIX}.directory`);
      }
      content.type = content.type === 'UNKNOWN' ? '--' : content.type;

      return content;
    });
  }

  handleSearch = (e) => {
    this.setState({search: e.target.value});
  }

  orderBy(sort) {
    if (sort !== this.state.sort) {
      this.setState({
        sort,
        sortOrder: 'asc'
      });
    } else {
      this.setState({
        sortOrder: this.state.sortOrder === 'asc' ? 'desc' : 'asc'
      });
    }
  }
  ingestFile(content) {
    let namespace = NamespaceStore.getState().selectedNamespace;
    let {scope} = this.props;
    let params = {
      namespace,
      path: content.path,
      lines: 10000,
      sampler: 'first'
    };

    if (scope) {
      if (typeof scope === 'string') {
        params.scope = scope;
      } else if (typeof scope === 'boolean') {
        // FIXME: Leaky. Why MMDS into filebrowser? This should be extracted out as a pure function.
        // Or when backend adds a flag to create a new workspace everytime then use that
        // instead of passing a hardcoded string for scope.
        params.scope = 'mmds';
      }
    }

    let headers = {
      'Content-Type': content.type
    };

    MyDataPrepApi.readFile(params, null, headers)
      .subscribe((res) => {
        let workspaceId = res.values[0].id;

        if (this.props.onWorkspaceCreate && typeof this.props.onWorkspaceCreate === 'function') {
          this.props.onWorkspaceCreate(workspaceId);
          return;
        }
        let navigatePath = `${window.location.origin}/cdap/ns/${namespace}/dataprep/${workspaceId}`;
        window.location.href = navigatePath;
      });

  }

  renderCollapsedContent(row) {
    return (
      <div
        key={row.uniqueId}
        className={classnames('row content-row', {
          'disabled': !row.directory && !row.wrangle
        })}
      >
        <div className="col-xs-8 name">
          <span
            className={classnames('type-icon fa fa-fw', {
              'folder-icon fa-folder-o': row.directory,
              'file-icon fa-file-o': !row.directory
            })}
          />
          <span title={row.name}>{row.name}</span>
        </div>
        <div className="col-xs-4">
          <span title={row.type}>
            {row.type}
          </span>
        </div>
      </div>
    );
  }

  renderRowContent(row) {
    if (this.props.noState || !this.props.enableRouting) {
      return this.renderCollapsedContent(row);
    }

    return (
      <div
        key={row.uniqueId}
        className={classnames('row content-row', {
          'disabled': !row.directory && !row.wrangle
        })}
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
          <span title={row.type}>
            {row.type}
          </span>
        </div>
        <div className="col-xs-1">
          <span title={row.displaySize}>
            {row.displaySize}
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
    if (this.props.noState || !this.props.enableRouting) {
      return (
        <div
          className="row-container"
          onClick={this.goToPath.bind(this, content.path)}
        >
          {this.renderRowContent(content)}
        </div>
      );
    }

    let linkPath = `${this.state.statePath}${content.path}`;
    return (
      <Link
        to={linkPath}
      >
        {this.renderRowContent(content)}
      </Link>
    );
  }

  renderFileContent(content) {
    return (
      <div
        className="row-container"
        onClick={this.ingestFile.bind(this, content)}
      >
        {this.renderRowContent(content)}
      </div>
    );
  }

  renderRow(content) {
    if (content.directory) {
      return this.renderDirectory(content);
    } else if (!content.wrangle) {
      return this.renderRowContent(content);
    } else {
      return this.renderFileContent(content);
    }
  }

  renderEmptySearch() {
    return (
      <div className="empty-search-container">
        <div className="empty-search">
          <strong>
            {T.translate(`${PREFIX}.EmptyMessage.title`, {searchText: this.state.search})}
          </strong>
          <hr />
          <span> {T.translate(`${PREFIX}.EmptyMessage.suggestionTitle`)} </span>
          <ul>
            <li>
              <span
                className="link-text"
                onClick={() => {
                  this.setState({
                    search: ''
                  });
                }}
              >
                {T.translate(`${PREFIX}.EmptyMessage.clearLabel`)}
              </span>
              <span> {T.translate(`${PREFIX}.EmptyMessage.suggestion1`)} </span>
            </li>
          </ul>
        </div>
      </div>
    );
  }

  renderContent() {
    if (this.state.loading) {
      // NEED TO REPLACE WITH ACTUAL LOADING ICON

      return (
        <LoadingSVGCentered />
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
        <h5 className="text-xs-center">{T.translate(`${PREFIX}.EmptyMessage.noFilesOrDirectories`)}</h5>
      );
    }

    let displayContent = this.state.contents;

    if (this.state.search.length > 0) {
      displayContent = this.state.contents.filter((content) => {
        let contentName = content.name.toLowerCase();
        let searchText = this.state.search.toLowerCase();

        return contentName.indexOf(searchText) !== -1;
      });

      if (displayContent.length === 0) {
        return this.renderEmptySearch();
      }
    }

    displayContent = orderBy(displayContent, [(content) => {
      let sortedItem = content[this.state.sort];
      if (typeof sortedItem !== 'string') {
        return sortedItem;

      }
      return sortedItem.toLowerCase();
    }], [this.state.sortOrder]);

    const TABLE_COLUMNS_PROPERTIES = {
      name: 'col-xs-3',
      type: 'col-xs-2',
      size: 'col-xs-1',
      'last-modified': 'col-xs-2',
      owner: 'col-xs-1',
      group: 'col-xs-1',
      permission: 'col-xs-2'
    };

    let columnProperties = TABLE_COLUMNS_PROPERTIES;

    if (this.props.noState || !this.props.enableRouting) {
      columnProperties = {
        name: 'col-xs-8',
        type: 'col-xs-4'
      };
    }

    const COLUMN_HEADERS = Object.keys(columnProperties);

    return (
      <div className="directory-content-table">
        <div className="content-header row">
          {
            COLUMN_HEADERS.map((head) => {
              return (
                <div
                  key={head}
                  className={columnProperties[head]}
                >
                  <span
                    onClick={this.orderBy.bind(this, head)}
                  >
                    {T.translate(`${PREFIX}.Table.${head}`)}

                    {
                      this.state.sort !== head ? null :
                      (
                        <span
                          className={classnames('fa sort-caret', {
                            'fa-caret-down': this.state.sortOrder === 'asc',
                            'fa-caret-up': this.state.sortOrder === 'desc'
                          })}
                        />
                      )
                    }
                  </span>
                </div>
              );
            })
          }
        </div>

        <div className="content-body clearfix">
          {
            displayContent.map((content) => {
              return this.renderRow(content);
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
              <span
                className="fa fa-fw"
                onClick={this.props.toggle}
              >
                <IconSVG name="icon-bars" />
              </span>

              <span>
                {this.props.browserTitle}
              </span>
            </h5>
          </div>
        </div>

        <div className="sub-panel">
          <div className="path-container">
            <FilePath
              baseStatePath={this.state.statePath}
              fullpath={this.state.path}
              enableRouting={this.props.enableRouting}
              onPathChange={this.goToPath}
            />
          </div>

          <div className="info-container">
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
                autoFocus={this.state.searchFocus}
              />
            </div>
          </div>
        </div>

        {this.renderContent()}
      </div>
    );
  }
}

