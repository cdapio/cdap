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
import {Provider} from 'react-redux';
import MyDataPrepApi from 'api/dataprep';
import NamespaceStore from 'services/NamespaceStore';
import classnames from 'classnames';
import {Link} from 'react-router-dom';
import {preventPropagation as preventPropagationService, objectQuery} from 'services/helpers';
import DataPrepBrowserStore from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';
import {setError, setADLSLoading, goToADLSfilePath, trimSuffixSlash} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import T from 'i18n-react';
import orderBy from 'lodash/orderBy';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import isEmpty from 'lodash/isEmpty';
import DataPrepStore from 'components/DataPrep/store';
import lastIndexOf from 'lodash/lastIndexOf';
import isNil from 'lodash/isNil';
import DataprepBrowserTopPanel from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserTopPanel';
import {ConnectionType} from 'components/DataPrepConnections/ConnectionType';
import DataPrepBrowserPageTitle from 'components/DataPrep/DataPrepBrowser/PageTitle';
import FilePath from 'components/FileBrowser/FilePath';
const BASEPATH = '/';
const PREFIX = 'features.ADLSBrowser';

export default class ADLSBrowser extends Component {

  state = {
    contents: [],
    path: '',
    statePath: objectQuery(this.props, 'match', 'url') || '',
    loading: true,
    search: '',
    sort: 'name',
    sortOrder: 'asc',
    searchFocus: true,
    connectionId: DataPrepBrowserStore.getState().adls.connectionId
  };

  static defaultProps = {
    enableRouting: true,
    scope: false,
    browserTitle: T.translate(`${PREFIX}.TopPanel.selectData`),
    allowSidePanelToggle: true
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
    browserTitle: PropTypes.string
  };

  parsePath() {
    this._isMounted = true;
    if (!this.props.enableRouting) {
      let path = this.getFilePath();
      this.dataprepSubscription = DataPrepStore.subscribe(() => {
        let path = this.getFilePath();
        if (!isNil(path) && path !== this.state.path) {
          goToADLSfilePath(path);
        }
      });
      goToADLSfilePath(path);
    } else {
      this.fetchADLSDirectory(this.props);
    }
  }

  componentDidMount() {
    this.parsePath();
    this.browserStoreSubscription = DataPrepBrowserStore.subscribe(() => {
      let {adls, activeBrowser} = DataPrepBrowserStore.getState();
      if (activeBrowser.name !== ConnectionType.ADLS) {
        return;
      }

      if (this._isMounted) {
        this.setState({
          contents: adls.contents,
          info: adls.info,
          connectionId: adls.connectionId,
          loading: adls.loading,
          path: adls.path,
          search: adls.search,
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
        goToADLSfilePath(path);
      }
    } else {
      this.fetchADLSDirectory(nextProps);
    }
  }

  getFilePath() {
    let {workspaceInfo} = DataPrepStore.getState().dataprep;
    let filePath = objectQuery(workspaceInfo, 'properties', 'path');
    filePath = !isEmpty(filePath) ? filePath.slice(0, lastIndexOf(filePath, '/') + 1) : this.state.path;
    if (isEmpty(filePath) || objectQuery(workspaceInfo, 'properties', 'connection') !== 'file') {
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

  fetchADLSDirectory(props) {
    let hdfsPath;

    if (this.props.noState) {
      hdfsPath = this.props.initialDirectoryPath;
      return;
    } else {
      if (objectQuery(props, 'match', 'url')) {
        let pathname = window.location.pathname.replace(/\/cdap/, '');
        hdfsPath = pathname.slice((window.knoxPrefix + props.match.url).length);
        hdfsPath = hdfsPath || this.props.initialDirectoryPath || BASEPATH;
      }
    }

    if (hdfsPath === this.state.path) { return; }

    if (hdfsPath) {
      goToADLSfilePath(hdfsPath);
    }
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
      lines: 1000,
      sampler: '',
      fraction: 10,
      connectionId: DataPrepBrowserStore.getState().adls.connectionId
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
      'Content-Type': 'text/plain'
    };
    setADLSLoading();
    MyDataPrepApi.adlsReadFile(params, null, headers)
      .subscribe((res) => {
        let workspaceId = res.values[0].id;

        if (this.props.onWorkspaceCreate && typeof this.props.onWorkspaceCreate === 'function') {
          this.props.onWorkspaceCreate(workspaceId);
          return;
        }
        let navigatePath = `${window.location.origin}/cdap/ns/${namespace}/dataprep/${workspaceId}`;
        window.location.href = navigatePath;
      }, (err) => {
        setError(err);
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
            {row.directory ? '--' : row.displaySize}
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
          key={content.uniqueId}
          className="row-container"
          onClick={goToADLSfilePath.bind(this, content.path)}
        >
          {this.renderRowContent(content)}
        </div>
      );
    }

    let linkPath = `${this.state.statePath}${content.path}/`;
    linkPath = trimSuffixSlash(linkPath);
    return (
      <Link
        key={content.uniqueId}
        to={linkPath}
      >
        {this.renderRowContent(content)}
      </Link>
    );
  }

  renderFileContent(content) {
    return (
      <div
        key={content.uniqueId}
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
              <span>{T.translate(`${PREFIX}.EmptyMessage.suggestion1`)}</span>
            </li>
          </ul>
        </div>
      </div>
    );
  }

  renderContent() {
    if (this.state.loading) {
      return (
        <LoadingSVGCentered />
      );
    }

    if (this.state.contents.length === 0) {
      return (
        <div className="empty-search-container">
          <div className="empty-search text-xs-center">
            <strong>
              {T.translate(`${PREFIX}.EmptyMessage.noFilesOrDirectories`)}
            </strong>
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
              content.wrangle = true;
              return this.renderRow(content);
            })
          }
        </div>
      </div>
    );
  }

  render() {
    return (
      <Provider store={DataPrepBrowserStore}>
        <div className="adls-browser file-browser-container">
          {
            this.props.enableRouting ?
              <DataPrepBrowserPageTitle
                browserI18NName="ADLSBrowser"
                browserStateName="adls"
                locationToPathInState={['prefix']}
              />
            :
              null
          }
          <DataprepBrowserTopPanel
            allowSidePanelToggle={true}
            toggle={this.props.toggle}
            browserTitle={this.props.browserTitle}
          />
          <div className={classnames("sub-panel", {'routing-disabled': !this.props.enableRouting})}>
            <div className="path-container">
              <FilePath
                baseStatePath={this.props.enableRouting ? this.props.match.url : '/'}
                enableRouting={this.props.enableRouting}
                fullpath={this.state.path}
                onPathChange={goToADLSfilePath}
              />
            </div>
            <div className="info-container"
                title={T.translate(`${PREFIX}.TopPanel.directoryMetrics`, {count: this.state.contents.length})}>
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
          <div className="adls-content">
            {this.renderContent()}
          </div>
        </div>
      </Provider>
    );
  }
}
