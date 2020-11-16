/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import classnames from 'classnames';
import { Link } from 'react-router-dom';
import FilePath from 'components/FileBrowser/FilePath';
import { preventPropagation as preventPropagationService, objectQuery } from 'services/helpers';
import DataPrepBrowserStore from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';
import {
  setError,
  goToPath,
  trimSuffixSlash,
} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import T from 'i18n-react';
import orderBy from 'lodash/orderBy';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import isEmpty from 'lodash/isEmpty';
import DataPrepStore from 'components/DataPrep/store';
import lastIndexOf from 'lodash/lastIndexOf';
import isNil from 'lodash/isNil';
import DataprepBrowserTopPanel from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserTopPanel';
import { ConnectionType } from 'components/DataPrepConnections/ConnectionType';
import history from 'services/history';
require('./FileBrowser.scss');

const BASEPATH = '/';
const PREFIX = 'features.FileBrowser';

export default class FileBrowser extends Component {
  state = {
    contents: [],
    path: '',
    statePath: objectQuery(this.props, 'match', 'url') || '',
    loading: true,
    search: '',
    sort: 'name',
    sortOrder: 'asc',
    searchFocus: true,
  };

  static defaultProps = {
    enableRouting: true,
    scope: false,
    browserTitle: T.translate(`${PREFIX}.TopPanel.selectData`),
    allowSidePanelToggle: true,
  };

  static propTypes = {
    allowSidePanelToggle: PropTypes.bool,
    location: PropTypes.object,
    match: PropTypes.object,
    initialDirectoryPath: PropTypes.string,
    noState: PropTypes.bool,
    toggle: PropTypes.func.isRequired,
    enableRouting: PropTypes.bool,
    onWorkspaceCreate: PropTypes.func,
    scope: PropTypes.oneOfType([PropTypes.bool, PropTypes.string]),
    browserTitle: PropTypes.string,
  };

  componentDidMount() {
    this._isMounted = true;
    if (!this.props.enableRouting) {
      let path = this.getFilePath();
      this.dataprepSubscription = DataPrepStore.subscribe(() => {
        let path = this.getFilePath();
        if (!isNil(path) && path !== this.state.path) {
          goToPath(path);
        }
      });
      goToPath(path);
    } else {
      this.fetchDirectory(this.props);
    }

    this.browserStoreSubscription = DataPrepBrowserStore.subscribe(() => {
      let { file, activeBrowser } = DataPrepBrowserStore.getState();
      if (activeBrowser.name !== ConnectionType.FILE) {
        return;
      }

      if (this._isMounted) {
        this.setState({
          contents: file.contents,
          loading: file.loading,
          path: file.path,
          search: file.search,
        });
      }
    });
  }

  componentWillUnmount() {
    this._isMounted = false;
    if (typeof this.dataprepSubscription === 'function') {
      this.dataprepSubscription();
    }
    if (typeof this.browserStoreSubscription === 'function') {
      this.browserStoreSubscription();
    }
    if (
      objectQuery(this.explorePathObservable, 'unsubscribe') &&
      typeof this.explorePathObservable.unsubscribe === 'function'
    ) {
      this.explorePathObservable.unsubscribe();
    }
  }

  componentWillReceiveProps(nextProps) {
    if (!this.props.enableRouting) {
      // When routing is disabled location, match are not entirely right.
      let path = this.getFilePath();
      if (!isNil(path) && path !== this.state.path) {
        goToPath(path);
      }
    } else {
      this.fetchDirectory(nextProps);
    }
  }

  getFilePath() {
    let { workspaceInfo } = DataPrepStore.getState().dataprep;
    // console.dir(workspaceInfo);
    let filePath = objectQuery(workspaceInfo, 'properties', 'path');
    filePath = !isEmpty(filePath)
      ? filePath.slice(0, lastIndexOf(filePath, '/') + 1)
      : this.state.path;
    if (isEmpty(filePath) || objectQuery(workspaceInfo, 'properties', 'connection') !== 'file') {
      filePath = BASEPATH;
    }
    return filePath;
  }

  getFileName() {
    const { workspaceInfo } = DataPrepStore.getState().dataprep;
    const fileName = objectQuery(workspaceInfo, 'properties', 'name');
    return fileName;
  }

  preventPropagation = (e) => {
    if (this.props.enableRouting) {
      return;
    }
    preventPropagationService(e);
  };

  fetchDirectory(props) {
    let hdfsPath;

    if (this.props.noState) {
      hdfsPath = this.props.initialDirectoryPath;
      return;
    } else {
      if (objectQuery(props, 'match', 'url')) {
        let pathname = window.location.pathname.replace(/\/cdap/, '');
        hdfsPath = pathname.slice(props.match.url.length);
        hdfsPath = hdfsPath || this.props.initialDirectoryPath || BASEPATH;
      }
    }

    if (hdfsPath === this.state.path) {
      return;
    }

    if (hdfsPath) {
      goToPath(hdfsPath);
    }
  }

  handleSearch = (e) => {
    this.setState({ search: e.target.value });
  };

  orderBy(sort) {
    if (sort !== this.state.sort) {
      this.setState({
        sort,
        sortOrder: 'asc',
      });
    } else {
      this.setState({
        sortOrder: this.state.sortOrder === 'asc' ? 'desc' : 'asc',
      });
    }
  }
  ingestFile(content) {
    const namespace = NamespaceStore.getState().selectedNamespace;
    const { scope } = this.props;
    const params = {
      context: namespace,
      path: content.path,
      lines: 10000,
      sampler: 'first',
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

    const headers = {
      'Content-Type': content.type,
    };

    MyDataPrepApi.readFile(params, null, headers).subscribe(
      (res) => {
        const workspaceId = res.values[0].id;

        if (this.props.onWorkspaceCreate && typeof this.props.onWorkspaceCreate === 'function') {
          this.props.onWorkspaceCreate(workspaceId);
          return;
        }
        history.push(`/ns/${namespace}/wrangler/${workspaceId}`);
      },
      (err) => {
        setError(err);
      }
    );
  }

  renderCollapsedContent(row, isSelected) {
    return (
      <div
        key={row.uniqueId}
        className={classnames('row content-row', {
          disabled: !row.directory && !row.wrangle,
          selected: isSelected,
        })}
      >
        <div className="col-8 name">
          <span
            className={classnames('type-icon fa fa-fw', {
              'folder-icon fa-folder-o': row.directory,
              'file-icon fa-file-o': !row.directory,
            })}
          />
          <span title={row.name}>{row.name}</span>
        </div>
        <div className="col-4">
          <span title={row.type}>{row.type}</span>
        </div>
      </div>
    );
  }

  renderRowContent(row, isSelected) {
    if (this.props.noState || !this.props.enableRouting) {
      return this.renderCollapsedContent(row, isSelected);
    }

    return (
      <div
        key={row.uniqueId}
        className={classnames('row content-row', {
          disabled: !row.directory && !row.wrangle,
          selected: isSelected,
        })}
      >
        <div className="col-3 name">
          <span
            className={classnames('type-icon fa fa-fw', {
              'folder-icon fa-folder-o': row.directory,
              'file-icon fa-file-o': !row.directory,
            })}
          />
          <span title={row.name}>{row.name}</span>
        </div>
        <div className="col-2">
          <span title={row.type}>{row.type}</span>
        </div>
        <div className="col-1">
          <span title={row.displaySize}>{row.directory ? '--' : row.displaySize}</span>
        </div>
        <div className="col-2">
          <span title={row['last-modified']}>{row['last-modified']}</span>
        </div>
        <div className="col-1">
          <span title={row.owner}>{row.owner}</span>
        </div>
        <div className="col-1">
          <span title={row.group}>{row.group}</span>
        </div>
        <div className="col-2">
          <span title={row.permission}>{row.permission}</span>
        </div>
      </div>
    );
  }

  renderDirectory(content) {
    if (this.props.noState || !this.props.enableRouting) {
      return (
        <div
          key={content.uniqueId}
          className="row-container"
          onClick={goToPath.bind(this, content.path)}
        >
          {this.renderRowContent(content, false)}
        </div>
      );
    }

    let linkPath = `${this.state.statePath}${content.path}`;
    linkPath = trimSuffixSlash(linkPath);
    return (
      <Link key={content.uniqueId} to={linkPath}>
        {this.renderRowContent(content, false)}
      </Link>
    );
  }

  renderFileContent(content, isSelected) {
    return (
      <div
        key={content.uniqueId}
        className="row-container"
        onClick={this.ingestFile.bind(this, content)}
      >
        {this.renderRowContent(content, isSelected)}
      </div>
    );
  }

  renderRow(content, isSelected) {
    if (content.directory) {
      return this.renderDirectory(content);
    } else if (!content.wrangle) {
      return this.renderRowContent(content, isSelected);
    } else {
      return this.renderFileContent(content, isSelected);
    }
  }

  renderEmptySearch() {
    return (
      <div className="empty-search-container">
        <div className="empty-search">
          <strong>
            {T.translate(`${PREFIX}.EmptyMessage.title`, { searchText: this.state.search })}
          </strong>
          <hr />
          <span> {T.translate(`${PREFIX}.EmptyMessage.suggestionTitle`)} </span>
          <ul>
            <li>
              <span
                className="link-text"
                onClick={() => {
                  this.setState({
                    search: '',
                  });
                }}
              >
                {T.translate(`${PREFIX}.EmptyMessage.clearLabel`)}
              </span>
              <span>{T.translate(`${PREFIX}.EmptyMessage.suggestion1`)}</span>
            </li>
          </ul>
        </div>
      </div>
    );
  }

  renderContent() {
    if (this.state.loading) {
      return <LoadingSVGCentered />;
    }

    if (this.state.contents.length === 0) {
      return (
        <div className="empty-search-container">
          <div className="empty-search text-center">
            <strong>{T.translate(`${PREFIX}.EmptyMessage.noFilesOrDirectories`)}</strong>
          </div>
        </div>
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

    displayContent = orderBy(
      displayContent,
      [
        (content) => {
          let sortedItem = content[this.state.sort];
          if (typeof sortedItem !== 'string') {
            return sortedItem;
          }
          return sortedItem.toLowerCase();
        },
      ],
      [this.state.sortOrder]
    );

    const TABLE_COLUMNS_PROPERTIES = {
      name: 'col-3',
      type: 'col-2',
      size: 'col-1',
      'last-modified': 'col-2',
      owner: 'col-1',
      group: 'col-1',
      permission: 'col-2',
    };

    let columnProperties = TABLE_COLUMNS_PROPERTIES;

    if (this.props.noState || !this.props.enableRouting) {
      columnProperties = {
        name: 'col-8',
        type: 'col-4',
      };
    }

    const COLUMN_HEADERS = Object.keys(columnProperties);

    let inWorkspaceDirectory = false;
    if (this.state.path === this.getFilePath()) {
      inWorkspaceDirectory = true;
    }

    return (
      <div className="directory-content-table">
        <div className="content-header row">
          {COLUMN_HEADERS.map((head) => {
            return (
              <div key={head} className={columnProperties[head]}>
                <span onClick={this.orderBy.bind(this, head)}>
                  {T.translate(`${PREFIX}.Table.${head}`)}

                  {this.state.sort !== head ? null : (
                    <span
                      className={classnames('fa sort-caret', {
                        'fa-caret-down': this.state.sortOrder === 'asc',
                        'fa-caret-up': this.state.sortOrder === 'desc',
                      })}
                    />
                  )}
                </span>
              </div>
            );
          })}
        </div>

        <div className="content-body clearfix">
          {displayContent.map((content) => {
            let isRowSelected = false;
            if (inWorkspaceDirectory && content.name === this.getFileName()) {
              isRowSelected = true;
            }
            return this.renderRow(content, isRowSelected);
          })}
        </div>
      </div>
    );
  }

  render() {
    return (
      <div className="file-browser-container">
        <DataprepBrowserTopPanel
          allowSidePanelToggle={this.props.allowSidePanelToggle}
          toggle={this.props.toggle}
          browserTitle={this.props.browserTitle}
        />

        <div className="sub-panel">
          <div className="path-container">
            <FilePath
              baseStatePath={this.state.statePath}
              fullpath={this.state.path}
              enableRouting={this.props.enableRouting}
              onPathChange={goToPath}
            />
          </div>

          <div
            className="info-container"
            title={T.translate(`${PREFIX}.TopPanel.directoryMetrics`, {
              count: this.state.contents.length,
            })}
          >
            <div className="info">
              <span>
                {T.translate(`${PREFIX}.TopPanel.directoryMetrics`, {
                  count: this.state.contents.length,
                })}
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
