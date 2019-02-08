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
import IconSVG from 'components/IconSVG';
import classnames from 'classnames';
import DataPrepBrowser from 'components/DataPrep/DataPrepBrowser';
import {
  setFileSystemAsActiveBrowser,
  setActiveBrowser,
  setS3AsActiveBrowser,
  setDatabaseAsActiveBrowser,
  setKafkaAsActiveBrowser,
  setGCSAsActiveBrowser,
  setBigQueryAsActiveBrowser,
  setSpannerAsActiveBrowser,
  reset as resetDataPrepBrowserStore,
} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import { Route, Switch, Redirect } from 'react-router-dom';
import { getCurrentNamespace } from 'services/NamespaceStore';
import T from 'i18n-react';
import LoadingSVG from 'components/LoadingSVG';
import MyDataPrepApi from 'api/dataprep';
import DataPrepServiceControl from 'components/DataPrep/DataPrepServiceControl';
import ConnectionsUpload from 'components/DataPrepConnections/UploadFile';
import AddConnection from 'components/DataPrepConnections/AddConnection';
import isNil from 'lodash/isNil';
import ExpandableMenu from 'components/UncontrolledComponents/ExpandableMenu';
import ConnectionPopover from 'components/DataPrepConnections/ConnectionPopover';
import DataPrepStore from 'components/DataPrep/store';
import { objectQuery, preventPropagation, isNilOrEmpty } from 'services/helpers';
import Helmet from 'react-helmet';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import queryString from 'query-string';
import Version from 'services/VersionRange/Version';
import { MIN_DATAPREP_VERSION } from 'components/DataPrep';
import NavLinkWrapper from 'components/NavLinkWrapper';
import { ConnectionType } from 'components/DataPrepConnections/ConnectionType';
import find from 'lodash/find';
import If from 'components/If';
import NoDefaultConnection from 'components/DataPrepConnections/NoDefaultConnection';
import isObject from 'lodash/isObject';
import DataPrepBrowserStore from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';
import { Theme } from 'services/ThemeHelper';

require('./DataPrepConnections.scss');
const PREFIX = 'features.DataPrepConnections';
const DATAPREP_I18N_PREFIX = 'features.DataPrep.pageTitle';

export default class DataPrepConnections extends Component {
  static propTypes = {
    match: PropTypes.object,
    location: PropTypes.object,
    enableRouting: PropTypes.bool,
    onWorkspaceCreate: PropTypes.func,
    singleWorkspaceMode: PropTypes.bool,
    sidePanelExpanded: PropTypes.bool,
    allowSidePanelToggle: PropTypes.bool,
    scope: PropTypes.oneOfType([PropTypes.bool, PropTypes.string]),
    browserTitle: PropTypes.string,
  };

  static defaultProps = {
    enableRouting: true,
    allowSidePanelToggle: true,
  };

  constructor(props) {
    super(props);

    let { workspaceInfo } = DataPrepStore.getState().dataprep;
    let activeConnectionType = objectQuery(workspaceInfo, 'properties', 'connection');
    let activeConnectionid = objectQuery(workspaceInfo, 'properties', 'connectionid');
    if (activeConnectionType) {
      activeConnectionType = ConnectionType[activeConnectionType.toUpperCase()];
    }
    this.state = {
      sidePanelExpanded: this.props.sidePanelExpanded || (this.props.enableRouting ? true : false),
      backendChecking: true,
      backendDown: false,
      loading: this.props.enableRouting ? true : false,
      connectionTypes: [],
      defaultConnection: null,
      databaseList: [],
      kafkaList: [],
      s3List: [],
      gcsList: [],
      bigQueryList: [],
      spannerList: [],
      activeConnectionid,
      activeConnectionType,
      showAddConnectionPopover: false,
      showUpload: activeConnectionType === 'UPLOAD' || false, // FIXME: This is used only when showing with no routing. We can do better.,
      redirectToDefaultConnectionOnDelete: false,
    };
  }

  componentDidMount() {
    this.checkBackendUp();
    if (!this.props.enableRouting) {
      this.dataprepSubscription = DataPrepStore.subscribe(() => {
        let { workspaceInfo } = DataPrepStore.getState().dataprep;

        if (
          objectQuery(workspaceInfo, 'properties', 'connectionid') !==
            this.state.activeConnectionid ||
          objectQuery(workspaceInfo, 'properties', 'id') !== this.state.activeConnectionid
        ) {
          let activeConnectionid =
            objectQuery(workspaceInfo, 'properties', 'connectionid') ||
            objectQuery(workspaceInfo, 'properties', 'id');
          let activeConnectionType = objectQuery(workspaceInfo, 'properties', 'connection');
          if (activeConnectionType) {
            activeConnectionType = ConnectionType[activeConnectionType.toUpperCase()];
          }
          this.setState({
            activeConnectionid,
            activeConnectionType,
            defaultConnection: activeConnectionid,
          });
        } else {
          this.fetchConnectionTypes();
        }
      });
    }
  }

  componentDidUpdate() {
    if (this.state.redirectToDefaultConnectionOnDelete) {
      let newState = {
        redirectToDefaultConnectionOnDelete: false,
      };
      if (!this.props.enableRouting) {
        newState = {
          ...newState,
          activeConnectionid: null,
          activeConnectionType: null,
        };
      }
      this.setState(newState, this.fetchConnectionTypes);
    }
  }

  componentWillUnmount() {
    if (typeof this.dataprepSubscription === 'function') {
      resetDataPrepBrowserStore();
      this.dataprepSubscription();
    }
  }

  checkBackendUp() {
    MyDataPrepApi.ping()
      .combineLatest(MyDataPrepApi.getApp())
      .subscribe(
        (res) => {
          const appSpec = res[1];

          let minimumVersion = new Version(MIN_DATAPREP_VERSION);

          if (minimumVersion.compareTo(new Version(appSpec.artifact.version)) > 0) {
            console.log('dataprep minimum version not met');

            this.setState({
              backendChecking: false,
              backendDown: true,
            });

            return;
          }

          this.setState({
            backendChecking: false,
            backendDown: false,
          });

          this.fetchConnectionTypes();
        },
        () => {
          this.setState({
            backendChecking: false,
            backendDown: true,
          });
        }
      );
  }

  handlePropagation(browserName, e) {
    if (this.props.enableRouting && !this.props.singleWorkspaceMode) {
      return;
    }
    preventPropagation(e);

    if (isNil(browserName)) {
      return;
    }

    // FIXME: This feels adhoc. We should be able to simplify this.
    if (typeof browserName === 'object' && browserName.type === ConnectionType.UPLOAD) {
      this.setState({
        showUpload: true,
      });
      return;
    }
    let activeConnectionType, activeConnectionid;
    if (typeof browserName === 'object' && browserName.type === ConnectionType.FILE) {
      setFileSystemAsActiveBrowser({ name: ConnectionType.FILE, path: '/' });
      activeConnectionType = ConnectionType.FILE;
    } else if (typeof browserName === 'object' && browserName.type === ConnectionType.DATABASE) {
      setDatabaseAsActiveBrowser({ name: ConnectionType.DATABASE, id: browserName.id });
      activeConnectionType = ConnectionType.DATABASE;
      activeConnectionid = browserName.id;
    } else if (typeof browserName === 'object' && browserName.type === ConnectionType.KAFKA) {
      setKafkaAsActiveBrowser({ name: ConnectionType.KAFKA, id: browserName.id });
      activeConnectionType = ConnectionType.KAFKA;
      activeConnectionid = browserName.id;
    } else if (typeof browserName === 'object' && browserName.type === ConnectionType.S3) {
      setS3AsActiveBrowser({ name: ConnectionType.S3, id: browserName.id, path: '/' });
      activeConnectionid = browserName.id;
      activeConnectionType = ConnectionType.S3;
    } else if (typeof browserName === 'object' && browserName.type === ConnectionType.GCS) {
      setGCSAsActiveBrowser({ name: ConnectionType.GCS, id: browserName.id, path: '/' });
      activeConnectionid = browserName.id;
      activeConnectionType = ConnectionType.GCS;
    } else if (typeof browserName === 'object' && browserName.type === ConnectionType.BIGQUERY) {
      setBigQueryAsActiveBrowser({ name: ConnectionType.BIGQUERY, id: browserName.id }, true);
      activeConnectionid = browserName.id;
      activeConnectionType = ConnectionType.BIGQUERY;
    } else if (typeof browserName === 'object' && browserName.type === ConnectionType.SPANNER) {
      setSpannerAsActiveBrowser({ name: ConnectionType.SPANNER, id: browserName.id }, true);
      activeConnectionid = browserName.id;
      activeConnectionType = ConnectionType.SPANNER;
    }

    this.setState({
      showUpload: false,
      activeConnectionType,
      activeConnectionid,
    });
  }

  onServiceStart = () => {
    this.checkBackendUp();
  };

  fetchConnectionTypes = () => {
    MyDataPrepApi.listConnectionTypes().subscribe(
      (res) => {
        this.setState({
          connectionTypes: res,
        });
        this.fetchConnectionsList();
        // TODO get default connection from backend and integrate.
      },
      (err) => {
        // If the user happens to use older dataprep artifact `/connectionTypes` won't exist.
        // So just query connections list instead of showing a spinner.
        if (err && err.statusCode === 404) {
          this.setState(
            {
              connectionTypes: Object.keys(ConnectionType).map((conn) => ({ type: conn })),
            },
            this.fetchConnectionsList
          );
        }
        console.log(err);
        // Will rebase with error handling PR to actually surface the error.
      }
    );
  };

  onActionFromConnectionsPopover = (action, connectionid) => {
    let state = DataPrepBrowserStore.getState();
    let activeBrowser;
    let activeConnectionId;
    if (state.activeBrowser.name) {
      activeBrowser = state.activeBrowser.name.toLowerCase();
      activeConnectionId = state[activeBrowser].connectionId;
    }
    // Check if the deleted connection is the active connection
    // If yes then redirect to default connection otherwise just reload connections.
    if (action === 'delete' && activeConnectionId === connectionid) {
      return this.setState({
        redirectToDefaultConnectionOnDelete: true,
      });
    }
    this.fetchConnectionsList();
  };

  fetchConnectionsList = () => {
    let namespace = getCurrentNamespace();

    MyDataPrepApi.listConnections({
      context: namespace,
      type: '*',
    }).subscribe((res) => {
      let state = {};

      if (res.default) {
        // Once we get a default connection from backend
        // set appropriate browser as active
        state.defaultConnection = res.default;
      }

      let databaseList = [],
        kafkaList = [],
        s3List = [],
        gcsList = [],
        bigQueryList = [],
        spannerList = [];

      if (!state.activeConnectionId && !state.activeConnectionType && state.defaultConnection) {
        let defaultConnectionObj = find(res.values, { id: state.defaultConnection });
        if (defaultConnectionObj) {
          state.activeConnectionid = objectQuery(defaultConnectionObj, 'id');
          state.activeConnectionType = objectQuery(defaultConnectionObj, 'type');
        }
      }

      res.values.forEach((connection) => {
        if (connection.type === ConnectionType.DATABASE) {
          databaseList.push(connection);
        } else if (connection.type === ConnectionType.KAFKA) {
          kafkaList.push(connection);
        } else if (connection.type === ConnectionType.S3) {
          s3List.push(connection);
        } else if (connection.type === ConnectionType.GCS) {
          gcsList.push(connection);
        } else if (connection.type === ConnectionType.BIGQUERY) {
          bigQueryList.push(connection);
        } else if (connection.type === ConnectionType.SPANNER) {
          spannerList.push(connection);
        }
      });

      state = {
        ...state,
        connectionsList: res.values,
        databaseList,
        kafkaList,
        s3List,
        gcsList,
        bigQueryList,
        spannerList,
        loading: false,
      };

      if (
        !state.defaultConnection &&
        this.props.singleWorkspaceMode &&
        this.props.sidePanelExpanded
      ) {
        state.sidePanelExpanded = true;
      }

      this.setState(state);
    });
  };

  toggleAddConnectionPopover = (showPopover = false) => {
    if (this.state.showAddConnectionPopover === showPopover) {
      return;
    }
    this.setState({
      showAddConnectionPopover: showPopover,
    });
  };

  toggleSidePanel = () => {
    if (!this.props.allowSidePanelToggle) {
      return;
    }
    this.setState({ sidePanelExpanded: !this.state.sidePanelExpanded });
  };

  onUploadSuccess = (workspaceId) => {
    if (this.props.enableRouting) {
      let namespace = getCurrentNamespace();

      let navigatePath = `${window.location.origin}/cdap/ns/${namespace}/dataprep/${workspaceId}`;
      window.location.href = navigatePath;
      return;
    }
    if (this.props.onWorkspaceCreate) {
      this.props.onWorkspaceCreate(workspaceId);
    }
  };

  renderDatabaseDetail() {
    let namespace = getCurrentNamespace();
    const baseLinkPath = `/ns/${namespace}/connections`;

    return (
      <div>
        {this.state.databaseList.map((database) => {
          return (
            <div key={database.id} title={database.name} className="clearfix">
              <NavLinkWrapper
                to={`${baseLinkPath}/database/${database.id}`}
                activeClassName="active"
                className={classnames('menu-item-expanded-list', {
                  active:
                    this.state.activeConnectionType === ConnectionType.DATABASE &&
                    this.state.activeConnectionid === database.name,
                })}
                isActive={(match) => {
                  return (
                    (this.state.activeConnectionType === ConnectionType.DATABASE &&
                      this.state.activeConnectionid === database.name) ||
                    match
                  );
                }}
                onClick={this.handlePropagation.bind(this, {
                  ...database,
                  name: ConnectionType.DATABASE,
                })}
                isNativeLink={this.props.singleWorkspaceMode}
              >
                {database.name}
              </NavLinkWrapper>

              <ConnectionPopover
                connectionInfo={database}
                onAction={this.onActionFromConnectionsPopover}
              />
            </div>
          );
        })}
      </div>
    );
  }

  renderKafkaDetail() {
    let namespace = getCurrentNamespace();
    const baseLinkPath = `/ns/${namespace}/connections`;

    return (
      <div>
        {this.state.kafkaList.map((kafka) => {
          return (
            <div key={kafka.id} title={kafka.name} className="clearfix">
              <NavLinkWrapper
                to={`${baseLinkPath}/kafka/${kafka.id}`}
                activeClassName="active"
                isActive={(match) => {
                  return (
                    (this.state.activeConnectionType === ConnectionType.KAFKA &&
                      this.state.activeConnectionid === kafka.name) ||
                    match
                  );
                }}
                className={classnames('menu-item-expanded-list', {
                  active:
                    this.state.activeConnectionType === ConnectionType.KAFKA &&
                    this.state.activeConnectionid === kafka.name,
                })}
                onClick={this.handlePropagation.bind(this, {
                  ...kafka,
                  name: ConnectionType.KAFKA,
                })}
                isNativeLink={this.props.singleWorkspaceMode}
              >
                {kafka.name}
              </NavLinkWrapper>

              <ConnectionPopover
                connectionInfo={kafka}
                onAction={this.onActionFromConnectionsPopover}
              />
            </div>
          );
        })}
      </div>
    );
  }

  renderS3Detail() {
    let namespace = getCurrentNamespace();
    const baseLinkPath = `/ns/${namespace}/connections`;

    return (
      <div>
        {this.state.s3List.map((s3) => {
          return (
            <div key={s3.id} title={s3.name} className="clearfix">
              <NavLinkWrapper
                to={`${baseLinkPath}/s3/${s3.id}`}
                activeClassName="active"
                isActive={(match) => {
                  return (
                    (this.state.activeConnectionType === ConnectionType.S3 &&
                      this.state.activeConnectionid === s3.name) ||
                    match
                  );
                }}
                className={classnames('menu-item-expanded-list', {
                  active:
                    this.state.activeConnectionType === ConnectionType.S3 &&
                    this.state.activeConnectionid === s3.name,
                })}
                onClick={this.handlePropagation.bind(this, { ...s3, name: ConnectionType.S3 })}
                isNativeLink={this.props.singleWorkspaceMode}
              >
                {s3.name}
              </NavLinkWrapper>

              <ConnectionPopover
                connectionInfo={s3}
                onAction={this.onActionFromConnectionsPopover}
              />
            </div>
          );
        })}
      </div>
    );
  }

  renderGCSDetail() {
    let namespace = getCurrentNamespace();
    const baseLinkPath = `/ns/${namespace}/connections`;

    return (
      <div>
        {this.state.gcsList.map((gcs) => {
          return (
            <div key={gcs.id} title={gcs.name} className="clearfix">
              <NavLinkWrapper
                to={`${baseLinkPath}/gcs/${gcs.id}`}
                activeClassName="active"
                isActive={(match) => {
                  return (
                    (this.state.activeConnectionType === ConnectionType.GCS &&
                      this.state.activeConnectionid === gcs.name) ||
                    match
                  );
                }}
                className={classnames('menu-item-expanded-list', {
                  active:
                    this.state.activeConnectionType === ConnectionType.GCS &&
                    this.state.activeConnectionid === gcs.name,
                })}
                onClick={this.handlePropagation.bind(this, { ...gcs, name: ConnectionType.GCS })}
                isNativeLink={this.props.singleWorkspaceMode}
              >
                {gcs.name}
              </NavLinkWrapper>

              <ConnectionPopover
                connectionInfo={gcs}
                onAction={this.onActionFromConnectionsPopover}
              />
            </div>
          );
        })}
      </div>
    );
  }

  renderBigQueryDetail() {
    let namespace = getCurrentNamespace();
    const baseLinkPath = `/ns/${namespace}/connections`;

    return (
      <div>
        {this.state.bigQueryList.map((bq) => {
          return (
            <div key={bq.id} title={bq.name} className="clearfix">
              <NavLinkWrapper
                to={`${baseLinkPath}/bigquery/${bq.id}`}
                activeClassName="active"
                isActive={(match) => {
                  return (
                    (this.state.activeConnectionType === ConnectionType.BIGQUERY &&
                      this.state.activeConnectionid === bq.name) ||
                    match
                  );
                }}
                className={classnames('menu-item-expanded-list', {
                  active:
                    this.state.activeConnectionType === ConnectionType.BIGQUERY &&
                    this.state.activeConnectionid === bq.name,
                })}
                onClick={this.handlePropagation.bind(this, {
                  ...bq,
                  name: ConnectionType.BIGQUERY,
                })}
                isNativeLink={this.props.singleWorkspaceMode}
              >
                {bq.name}
              </NavLinkWrapper>

              <ConnectionPopover
                connectionInfo={bq}
                onAction={this.onActionFromConnectionsPopover}
              />
            </div>
          );
        })}
      </div>
    );
  }

  renderSpannerDetail() {
    let namespace = getCurrentNamespace();
    const baseLinkPath = `/ns/${namespace}/connections`;

    return (
      <div>
        {this.state.spannerList.map((spanner) => {
          return (
            <div key={spanner.id} title={spanner.name} className="clearfix">
              <NavLinkWrapper
                to={`${baseLinkPath}/spanner/${spanner.id}`}
                activeClassName="active"
                isActive={(match) => {
                  return (
                    (this.state.activeConnectionType === ConnectionType.SPANNER &&
                      this.state.activeConnectionid === spanner.name) ||
                    match
                  );
                }}
                className={classnames('menu-item-expanded-list', {
                  active:
                    this.state.activeConnectionType === ConnectionType.SPANNER &&
                    this.state.activeConnectionid === spanner.name,
                })}
                onClick={this.handlePropagation.bind(this, {
                  ...spanner,
                  name: ConnectionType.SPANNER,
                })}
                isNativeLink={this.props.singleWorkspaceMode}
              >
                {spanner.name}
              </NavLinkWrapper>

              <ConnectionPopover
                connectionInfo={spanner}
                onAction={this.onActionFromConnectionsPopover}
              />
            </div>
          );
        })}
      </div>
    );
  }

  renderPanel() {
    if (!this.state.sidePanelExpanded) {
      return null;
    }

    let namespace = getCurrentNamespace();
    const baseLinkPath = `/ns/${namespace}/connections`;

    return (
      <div className="connections-panel">
        <div className="panel-title" onClick={this.toggleSidePanel}>
          <h5>
            <span className="fa fa-fw">
              <IconSVG name="icon-angle-double-left" />
            </span>

            <span>{T.translate(`${PREFIX}.title`, { namespace })}</span>
          </h5>
        </div>

        <div className="connections-menu">
          <If condition={find(this.state.connectionTypes, { type: ConnectionType.UPLOAD })}>
            <div className="menu-item">
              <NavLinkWrapper
                to={`${baseLinkPath}/upload`}
                activeClassName="active"
                onClick={this.handlePropagation.bind(this, { type: ConnectionType.UPLOAD })}
                isNativeLink={this.props.singleWorkspaceMode}
              >
                <span className="fa fa-fw">
                  <IconSVG name="icon-upload" />
                </span>

                <span>{T.translate(`${PREFIX}.upload`)}</span>
              </NavLinkWrapper>
            </div>
          </If>

          <If condition={find(this.state.connectionTypes, { type: ConnectionType.FILE })}>
            <div className="menu-item">
              <NavLinkWrapper
                to={`${baseLinkPath}/file`}
                activeClassName="active"
                /** This is here for non-react environments. Coming from pipeline to dataprep */
                className={this.state.activeConnectionType === ConnectionType.FILE ? 'active' : ''}
                isActive={(match) => {
                  return this.state.activeConnectionType === ConnectionType.FILE || match;
                }}
                onClick={this.handlePropagation.bind(this, { type: ConnectionType.FILE })}
                isNativeLink={this.props.singleWorkspaceMode}
              >
                <span className="fa fa-fw">
                  <IconSVG name="icon-hdfs" />
                </span>

                <span>{T.translate(`${PREFIX}.hdfs`)}</span>
              </NavLinkWrapper>
            </div>
          </If>

          <If condition={find(this.state.connectionTypes, { type: ConnectionType.DATABASE })}>
            <ExpandableMenu>
              <div>
                <span className="fa fa-fw">
                  <IconSVG name="icon-database" />
                </span>
                <span>
                  {T.translate(`${PREFIX}.database`, { count: this.state.databaseList.length })}
                </span>
              </div>
              {this.renderDatabaseDetail()}
            </ExpandableMenu>
          </If>

          <If condition={find(this.state.connectionTypes, { type: ConnectionType.KAFKA })}>
            <ExpandableMenu>
              <div>
                <span className="fa fa-fw">
                  <IconSVG name="icon-kafka" />
                </span>
                <span>
                  {T.translate(`${PREFIX}.kafka`, { count: this.state.kafkaList.length })}
                </span>
              </div>
              {this.renderKafkaDetail()}
            </ExpandableMenu>
          </If>

          <If condition={find(this.state.connectionTypes, { type: ConnectionType.S3 })}>
            <ExpandableMenu>
              <div>
                <span className="fa fa-fw">
                  <IconSVG name="icon-s3" />
                </span>
                <span>{T.translate(`${PREFIX}.s3`, { count: this.state.s3List.length })}</span>
              </div>
              {this.renderS3Detail()}
            </ExpandableMenu>
          </If>

          <If condition={find(this.state.connectionTypes, { type: ConnectionType.GCS })}>
            <ExpandableMenu>
              <div>
                <span className="fa fa-fw">
                  <IconSVG name="icon-storage" />
                </span>
                <span>{T.translate(`${PREFIX}.gcs`, { count: this.state.gcsList.length })}</span>
              </div>
              {this.renderGCSDetail()}
            </ExpandableMenu>
          </If>

          <If condition={find(this.state.connectionTypes, { type: ConnectionType.BIGQUERY })}>
            <ExpandableMenu>
              <div>
                <span className="fa fa-fw">
                  <IconSVG name="icon-bigquery" />
                </span>
                <span>
                  {T.translate(`${PREFIX}.bigquery`, { count: this.state.bigQueryList.length })}
                </span>
              </div>
              {this.renderBigQueryDetail()}
            </ExpandableMenu>
          </If>

          <If condition={find(this.state.connectionTypes, { type: ConnectionType.SPANNER })}>
            <ExpandableMenu>
              <div>
                <span className="fa fa-fw">
                  <IconSVG name="icon-spanner" />
                </span>
                <span>
                  {T.translate(`${PREFIX}.spanner`, { count: this.state.spannerList.length })}
                </span>
              </div>
              {this.renderSpannerDetail()}
            </ExpandableMenu>
          </If>
        </div>

        <AddConnection
          onAdd={this.fetchConnectionsList}
          validConnectionTypes={this.state.connectionTypes}
          showPopover={this.state.showAddConnectionPopover}
          onPopoverClose={this.toggleAddConnectionPopover.bind(this, false)}
        />
      </div>
    );
  }

  renderRoutes() {
    const BASEPATH = '/ns/:namespace/connections';
    if (this.state.redirectToDefaultConnectionOnDelete) {
      return <Redirect to={`/ns/${getCurrentNamespace()}/connections`} />;
    }
    return (
      <Switch>
        <Route
          path={`${BASEPATH}/file`}
          render={({ match }) => {
            const setActiveConnection = setActiveBrowser.bind(null, { name: ConnectionType.FILE });
            return (
              <DataPrepBrowser
                match={match}
                toggle={this.toggleSidePanel}
                onWorkspaceCreate={this.onUploadSuccess}
                setActiveConnection={setActiveConnection}
              />
            );
          }}
        />
        <Route
          path={`${BASEPATH}/upload`}
          render={() => {
            return (
              <ConnectionsUpload
                toggle={this.toggleSidePanel}
                onWorkspaceCreate={this.onUploadSuccess}
              />
            );
          }}
        />
        <Route
          path={`${BASEPATH}/database/:databaseId`}
          render={(match) => {
            let id = match.match.params.databaseId;
            const setActiveConnection = setDatabaseAsActiveBrowser.bind(null, {
              name: ConnectionType.DATABASE,
              id,
            });
            return (
              <DataPrepBrowser
                match={match}
                toggle={this.toggleSidePanel}
                onWorkspaceCreate={this.onUploadSuccess}
                setActiveConnection={setActiveConnection}
              />
            );
          }}
        />
        <Route
          path={`${BASEPATH}/kafka/:kafkaId`}
          render={(match) => {
            let id = match.match.params.kafkaId;
            const setActiveConnection = setKafkaAsActiveBrowser.bind(null, {
              name: ConnectionType.KAFKA,
              id,
            });
            return (
              <DataPrepBrowser
                match={match}
                toggle={this.toggleSidePanel}
                onWorkspaceCreate={this.onUploadSuccess}
                setActiveConnection={setActiveConnection}
              />
            );
          }}
        />
        <Route
          path={`${BASEPATH}/s3/:s3Id`}
          render={(match) => {
            let id = match.match.params.s3Id;
            let { prefix = '/' } = queryString.parse(match.location.search);
            const setActiveConnection = setS3AsActiveBrowser.bind(null, {
              name: ConnectionType.S3,
              id,
              path: prefix,
            });
            return (
              <DataPrepBrowser
                match={match}
                toggle={this.toggleSidePanel}
                onWorkspaceCreate={this.onUploadSuccess}
                setActiveConnection={setActiveConnection}
              />
            );
          }}
        />
        <Route
          path={`${BASEPATH}/gcs/:gcsId`}
          render={(match) => {
            let id = match.match.params.gcsId;
            let { prefix = '/' } = queryString.parse(match.location.search);
            const setActiveConnection = setGCSAsActiveBrowser.bind(null, {
              name: ConnectionType.GCS,
              id,
              path: prefix,
            });
            return (
              <DataPrepBrowser
                match={match}
                toggle={this.toggleSidePanel}
                onWorkspaceCreate={this.onUploadSuccess}
                setActiveConnection={setActiveConnection}
              />
            );
          }}
        />
        <Route
          path={`${BASEPATH}/bigquery/:bigQueryId`}
          render={(match) => {
            let id = match.match.params.bigQueryId;
            const setActiveConnection = setBigQueryAsActiveBrowser.bind(null, {
              name: ConnectionType.BIGQUERY,
              id,
            });
            return (
              <DataPrepBrowser
                match={match}
                toggle={this.toggleSidePanel}
                onWorkspaceCreate={this.onUploadSuccess}
                setActiveConnection={setActiveConnection}
              />
            );
          }}
        />
        <Route
          path={`${BASEPATH}/spanner/:spannerId`}
          render={({ match }) => {
            let id = match.params.spannerId;
            const setActiveConnection = setSpannerAsActiveBrowser.bind(null, {
              name: ConnectionType.SPANNER,
              id,
            });
            return (
              <DataPrepBrowser
                match={match}
                toggle={this.toggleSidePanel}
                onWorkspaceCreate={this.onUploadSuccess}
                setActiveConnection={setActiveConnection}
              />
            );
          }}
        />
        <Route
          render={() => {
            let doesFileExists = find(this.state.connectionTypes, { type: ConnectionType.FILE });
            if (!this.state.defaultConnection && doesFileExists) {
              /*
              If the dataprep is configured with a list of valid connection types
              and has a default connection this won't be a problem.

              This condition will be true in a sandbox environment where there is no
              config for the dataprep app that defines valid connection types and a
              default connection.
              If there is no default connection AND if the file connection type is valid
              then go to file browser.
            */
              return <Redirect to={`/ns/${getCurrentNamespace()}/connections/file`} />;
            }
            return (
              <NoDefaultConnection
                defaultConnection={this.state.defaultConnection}
                connectionsList={this.state.connectionsList}
                showAddConnectionPopover={this.toggleAddConnectionPopover.bind(this, true)}
                toggleSidepanel={this.toggleSidePanel}
              />
            );
          }}
        />
      </Switch>
    );
  }

  showNonRoutableContents() {
    if (this.state.redirectToDefaultConnectionOnDelete) {
      return null;
    }
    if (this.state.showUpload) {
      return (
        <ConnectionsUpload toggle={this.toggleSidePanel} onWorkspaceCreate={this.onUploadSuccess} />
      );
    }
    let { enableRouting, ...attributes } = this.props;
    enableRouting = this.props.singleWorkspaceMode ? false : this.props.enableRouting;
    let setActiveConnection;
    let {
      activeConnectionType,
      activeConnectionid,
      defaultConnection,
      connectionTypes,
      connectionsList,
    } = this.state;
    let defaultConnectionObj = find(connectionsList, { id: defaultConnection });
    defaultConnection = isNilOrEmpty(defaultConnectionObj) ? null : defaultConnection;
    if (this.state.activeConnectionType === ConnectionType.DATABASE) {
      setActiveConnection = setDatabaseAsActiveBrowser.bind(null, {
        name: ConnectionType.DATABASE,
        id: this.state.activeConnectionid,
      });
    } else if (this.state.activeConnectionType === ConnectionType.KAFKA) {
      setActiveConnection = setKafkaAsActiveBrowser.bind(null, {
        name: ConnectionType.KAFKA,
        id: this.state.activeConnectionid,
      });
    } else if (this.state.activeConnectionType === ConnectionType.FILE) {
      setActiveConnection = setActiveBrowser.bind(null, { name: ConnectionType.FILE });
    } else if (this.state.activeConnectionType === ConnectionType.S3) {
      let { workspaceInfo } = DataPrepStore.getState().dataprep;
      let { s3 } = DataPrepBrowserStore.getState();
      // So the code path below will set the path of the s3 browser to current path,
      // when opening up connections from S3 workspace. However, this function is also
      // called when the user clicks on another S3 connection from this view :(. So
      // this if condition is to make sure we only set path to current workspace path
      // when the user is going back to connections view, not when they select another
      // S3 connection
      if (
        !s3.connectionId ||
        s3.connectionId === objectQuery(workspaceInfo, 'properties', 'connectionId')
      ) {
        let path = '/';
        if (isObject(workspaceInfo)) {
          let { key } = workspaceInfo.properties;
          let bucketName = workspaceInfo.properties['bucket-name'];
          if (bucketName) {
            path = `/${bucketName}/${key}`;
          } else {
            let state = DataPrepBrowserStore.getState();
            path = state.s3.prefix;
          }
        }
        setActiveConnection = setS3AsActiveBrowser.bind(null, {
          name: ConnectionType.S3,
          id: this.state.activeConnectionid,
          path,
        });
      }
    } else if (this.state.activeConnectionType === ConnectionType.GCS) {
      let { workspaceInfo } = DataPrepStore.getState().dataprep;
      let { gcs } = DataPrepBrowserStore.getState();
      // Same as S3 connection above
      if (
        !gcs.connectionId ||
        gcs.connectionId === objectQuery(workspaceInfo, 'properties', 'connectionId')
      ) {
        let path = '/';
        if (isObject(workspaceInfo) && workspaceInfo.properties.path) {
          path = workspaceInfo.properties.path;
          path = path.split('/');
          path = path.slice(0, path.length - 1).join('/');
          let bucketName = workspaceInfo.properties.bucket;
          if (bucketName) {
            path = !isNilOrEmpty(path) ? `/${bucketName}/${path}/` : `/${bucketName}`;
          } else {
            let state = DataPrepBrowserStore.getState();
            path = state.gcs.prefix;
          }
        }
        setActiveConnection = setGCSAsActiveBrowser.bind(null, {
          name: ConnectionType.GCS,
          id: this.state.activeConnectionid,
          path,
        });
      }
    } else if (this.state.activeConnectionType === ConnectionType.BIGQUERY) {
      setActiveConnection = setBigQueryAsActiveBrowser.bind(
        null,
        { name: ConnectionType.BIGQUERY, id: this.state.activeConnectionid },
        true
      );
    } else if (this.state.activeConnectionType === ConnectionType.SPANNER) {
      setActiveConnection = setSpannerAsActiveBrowser.bind(
        null,
        { name: ConnectionType.SPANNER, id: this.state.activeConnectionid },
        true
      );
    }

    const isFileConnectionValid = find(connectionTypes, { type: ConnectionType.FILE });
    if (!activeConnectionType && !activeConnectionid && !defaultConnection) {
      if (isFileConnectionValid) {
        setActiveBrowser({ name: ConnectionType.FILE });
      } else {
        return (
          <NoDefaultConnection
            defaultConnection={this.state.defaultConnection}
            connectionsList={this.state.connectionsList}
            showAddConnectionPopover={this.toggleAddConnectionPopover.bind(this, true)}
            toggleSidepanel={this.toggleSidePanel}
          />
        );
      }
    }
    return (
      <DataPrepBrowser
        match={this.props.match}
        location={this.props.location}
        toggle={this.toggleSidePanel}
        onWorkspaceCreate={!this.props.singleWorkspaceMode ? null : this.props.onWorkspaceCreate}
        enableRouting={enableRouting}
        setActiveConnection={setActiveConnection}
        {...attributes}
      />
    );
  }

  renderContent = () => {
    if (this.props.enableRouting && !this.props.singleWorkspaceMode) {
      return this.renderRoutes();
    }
    return this.showNonRoutableContents();
  };

  render() {
    const featureName = Theme.featureNames.dataPrep;
    let pageTitle = (
      <Helmet
        title={T.translate(DATAPREP_I18N_PREFIX, {
          productName: Theme.productName,
          featureName,
        })}
      />
    );
    if (this.state.backendChecking) {
      return (
        <div className="text-center">
          {this.props.singleWorkspaceMode || this.props.enableRouting ? null : pageTitle}
          <LoadingSVG />
        </div>
      );
    }

    if (this.state.backendDown) {
      return (
        <div>
          {this.props.singleWorkspaceMode || this.props.enableRouting ? null : pageTitle}
          <DataPrepServiceControl onServiceStart={this.onServiceStart} />
        </div>
      );
    }

    let { backendChecking, loading } = this.state;
    if (backendChecking || loading) {
      return <LoadingSVGCentered />;
    }
    return (
      <div className="dataprep-connections-container">
        {this.props.enableRouting ? (
          <Helmet
            title={T.translate(`${PREFIX}.pageTitle`, {
              productName: Theme.productName,
            })}
          />
        ) : null}
        {this.props.singleWorkspaceMode || this.props.enableRouting ? null : pageTitle}
        {this.renderPanel()}

        <div
          className={classnames('connections-content', {
            expanded: !this.state.sidePanelExpanded,
          })}
        >
          {this.renderContent()}
        </div>
      </div>
    );
  }
}
