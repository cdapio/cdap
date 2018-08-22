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
  setActiveBrowser,
  setS3AsActiveBrowser,
  setDatabaseAsActiveBrowser,
  setKafkaAsActiveBrowser,
  setGCSAsActiveBrowser,
  setBigQueryAsActiveBrowser,
  listBigQueryTables,
  reset as resetDataPrepBrowserStore,
} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import {Route, Redirect, Switch} from 'react-router-dom';
import {getCurrentNamespace} from 'services/NamespaceStore';
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
import {objectQuery, preventPropagation} from 'services/helpers';
import Helmet from 'react-helmet';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import queryString from 'query-string';
import Version from 'services/VersionRange/Version';
import { MIN_DATAPREP_VERSION } from 'components/DataPrep';
import NavLinkWrapper from 'components/NavLinkWrapper';

require('./DataPrepConnections.scss');
const PREFIX = 'features.DataPrepConnections';
const DATAPREP_I18N_PREFIX = 'features.DataPrep.pageTitle';

const RouteToHDFS = () => {
  let namespace = getCurrentNamespace();

  return (
    <Redirect to={`/ns/${namespace}/connections/browser`} />
  );
};

export default class DataPrepConnections extends Component {
  state = {
    sidePanelExpanded: this.props.sidePanelExpanded || (this.props.enableRouting ? true : false),
    backendChecking: true,
    backendDown: false,
    loading: this.props.enableRouting ? true : false,
    databaseList: [],
    kafkaList: [],
    s3List: [],
    gcsList: [],
    bigQueryList: [],
    activeConnectionid: objectQuery(DataPrepStore.getState().dataprep, 'workspaceInfo', 'properties', 'connectionid'),
    activeConnectionType: objectQuery(DataPrepStore.getState().dataprep, 'workspaceInfo', 'properties', 'connection'),
    showUpload: false // FIXME: This is used only when showing with no routing. We can do better.
  };

  componentDidMount() {
    this.checkBackendUp();
    if (isNil(this.props.match) || this.props.match.path.indexOf('connections') === -1) {
      setActiveBrowser({name: 'file'});
    }
    if (!this.props.enableRouting) {
      this.dataprepSubscription = DataPrepStore.subscribe(() => {
        let {workspaceInfo} = DataPrepStore.getState().dataprep;

        if (
          objectQuery(workspaceInfo, 'properties', 'connectionid') !== this.state.activeConnectionid ||
          objectQuery(workspaceInfo, 'properties', 'id') !== this.state.activeConnectionid
        ) {
          this.setState({
            activeConnectionid: objectQuery(workspaceInfo, 'properties', 'connectionid') || objectQuery(workspaceInfo, 'properties', 'id'),
            activeConnectionType: objectQuery(workspaceInfo, 'properties', 'connection')
          });
        }
      });
    }
  }

  componentWillUnmount() {
    if (typeof this.dataprepSubscription === 'function') {
      resetDataPrepBrowserStore();
      this.dataprepSubscription();
    }
  }
  checkBackendUp() {
    let namespace = getCurrentNamespace();

    MyDataPrepApi.ping({ namespace })
      .combineLatest(MyDataPrepApi.getApp({ namespace }))
      .subscribe((res) => {
        const appSpec = res[1];

        let minimumVersion = new Version(MIN_DATAPREP_VERSION);

        if (minimumVersion.compareTo(new Version(appSpec.artifactVersion)) > 0) {
          console.log('dataprep minimum version not met');

          this.setState({
            backendChecking: false,
            backendDown: true
          });

          return;
        }


        this.setState({
          backendChecking: false,
          backendDown: false
        });

        this.fetchConnectionsList();
      }, (err) => {
        if (err.statusCode === 503) {
          console.log('backend not started');

          this.setState({
            backendChecking: false,
            backendDown: true
          });

          return;
        }
      });
  }

  handlePropagation(browserName, e) {
    if (this.props.enableRouting && !this.props.singleWorkspaceMode) {
      setActiveBrowser({name: typeof browserName === 'object' ? browserName.name : browserName});
      return;
    }
    preventPropagation(e);

    if (isNil(browserName)) {
      return;
    }

    // FIXME: This feels adhoc. We should be able to simplify this.
    if (typeof browserName === 'object' && browserName.type === 'upload') {
      this.setState({
        showUpload: true
      });
      return;
    }
    let activeConnectionType, activeConnectionid;
    if (typeof browserName === 'object' && browserName.type === 'file') {
      setActiveBrowser({name: 'file'});
      activeConnectionType = 'file';
    } else if (typeof browserName === 'object' && browserName.type === 'DATABASE') {
      setDatabaseAsActiveBrowser({name: 'database', id: browserName.id});
      activeConnectionType = 'database';
      activeConnectionid = browserName.id;
    } else if (typeof browserName === 'object' && browserName.type === 'KAFKA') {
      setKafkaAsActiveBrowser({name: 'kafka', id: browserName.id});
      activeConnectionType = 'kafka';
      activeConnectionid = browserName.id;
    } else if (typeof browserName === 'object' && browserName.type === 'S3') {
      setS3AsActiveBrowser({name: 's3', id: browserName.id, path: '/'});
    } else if (typeof browserName === 'object' && browserName.type === 'GCS') {
      setGCSAsActiveBrowser({name: 'gcs', id: browserName.id, path: '/'});
    } else if (typeof browserName === 'object' && browserName.type === 'BIGQUERY') {
      setBigQueryAsActiveBrowser({name: 'bigquery', id: browserName.id});
    }

    this.setState({
      showUpload: false,
      activeConnectionType,
      activeConnectionid
    });
  }

  onServiceStart = () => {
    this.checkBackendUp();
  };

  fetchConnectionsList = (action, targetId) => {
    let namespace = getCurrentNamespace();

    MyDataPrepApi.listConnections({
      namespace,
      type: '*' // currently only going to fetch database connection
    }).subscribe((res) => {
      // need to group by connection type

      let databaseList = [],
          kafkaList = [],
          s3List = [],
          gcsList = [],
          bigQueryList = [];

      let state = {};
      if (action === 'delete' && this.state.activeConnectionid === targetId) {
        state.activeConnectionid = null;
        state.activeConnectionType = 'file';
        setActiveBrowser({name: 'file'});
      }

      res.values.forEach((connection) => {
        if (connection.type === 'DATABASE') {
          databaseList.push(connection);
        } else if (connection.type === 'KAFKA') {
          kafkaList.push(connection);
        } else if (connection.type === 'S3') {
          s3List.push(connection);
        } else if (connection.type === 'GCS') {
          gcsList.push(connection);
        } else if (connection.type === 'BIGQUERY') {
          bigQueryList.push(connection);
        }
      });

      state = {
        ...state,
        databaseList,
        kafkaList,
        s3List,
        gcsList,
        bigQueryList,
        loading: false
      };

      this.setState(state);
    });
  }

  toggleSidePanel = () => {
    if (!this.props.allowSidePanelToggle) {
      return;
    }
    this.setState({sidePanelExpanded: !this.state.sidePanelExpanded});
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
            <div
              key={database.id}
              title={database.name}
              className="clearfix"
            >
              <NavLinkWrapper
                to={`${baseLinkPath}/database/${database.id}`}
                activeClassName="active"
                className="menu-item-expanded-list"
                onClick={this.handlePropagation.bind(this, database)}
                isNativeLink={this.props.singleWorkspaceMode}
              >
                {database.name}
              </NavLinkWrapper>

              <ConnectionPopover
                connectionInfo={database}
                onAction={this.fetchConnectionsList}
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
            <div
              key={kafka.id}
              title={kafka.name}
              className="clearfix"
            >
              <NavLinkWrapper
                to={`${baseLinkPath}/kafka/${kafka.id}`}
                activeClassName="active"
                className="menu-item-expanded-list"
                onClick={this.handlePropagation.bind(this, {...kafka, name: 'kafka'})}
                isNativeLink={this.props.singleWorkspaceMode}
              >
                {kafka.name}
              </NavLinkWrapper>

              <ConnectionPopover
                connectionInfo={kafka}
                onAction={this.fetchConnectionsList}
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
            <div
              key={s3.id}
              title={s3.name}
              className="clearfix"
            >
              <NavLinkWrapper
                to={`${baseLinkPath}/s3/${s3.id}`}
                activeClassName="active"
                className="menu-item-expanded-list"
                onClick={this.handlePropagation.bind(this, {...s3, name: s3.type.toLowerCase()})}
                isNativeLink={this.props.singleWorkspaceMode}
              >
                {s3.name}
              </NavLinkWrapper>

              <ConnectionPopover
                connectionInfo={s3}
                onAction={this.fetchConnectionsList}
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
            <div
              key={gcs.id}
              title={gcs.name}
              className="clearfix"
            >
              <NavLinkWrapper
                to={`${baseLinkPath}/gcs/${gcs.id}`}
                activeClassName="active"
                className="menu-item-expanded-list"
                onClick={this.handlePropagation.bind(this, {...gcs, name: gcs.type.toLowerCase()})}
                isNativeLink={this.props.singleWorkspaceMode}
              >
                {gcs.name}
              </NavLinkWrapper>

              <ConnectionPopover
                connectionInfo={gcs}
                onAction={this.fetchConnectionsList}
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
            <div
              key={bq.id}
              title={bq.name}
              className="clearfix"
            >
              <NavLinkWrapper
                to={`${baseLinkPath}/bigquery/${bq.id}`}
                activeClassName="active"
                className="menu-item-expanded-list"
                onClick={this.handlePropagation.bind(this, {...bq, name: bq.type.toLowerCase()})}
                isNativeLink={this.props.singleWorkspaceMode}
              >
                {bq.name}
              </NavLinkWrapper>

              <ConnectionPopover
                connectionInfo={bq}
                onAction={this.fetchConnectionsList}
              />
            </div>
          );
        })}
      </div>
    );
  }

  renderPanel() {
    if (!this.state.sidePanelExpanded)  { return null; }

    let namespace = getCurrentNamespace();
    const baseLinkPath = `/ns/${namespace}/connections`;

    return (
      <div className="connections-panel">
        <div
          className="panel-title"
          onClick={this.toggleSidePanel}
        >
          <h5>
            <span className="fa fa-fw">
              <IconSVG name="icon-angle-double-left" />
            </span>

            <span>
              {T.translate(`${PREFIX}.title`, { namespace })}
            </span>
          </h5>
        </div>

        <div className="connections-menu">
          <div className="menu-item">
            <NavLinkWrapper
              to={`${baseLinkPath}/upload`}
              activeClassName="active"
              onClick={this.handlePropagation.bind(this, {type: 'upload'})}
              isNativeLink={this.props.singleWorkspaceMode}
            >
              <span className="fa fa-fw">
                <IconSVG name="icon-upload" />
              </span>

              <span>
                {T.translate(`${PREFIX}.upload`)}
              </span>
            </NavLinkWrapper>
          </div>

          <div className="menu-item">
            <NavLinkWrapper
              to={`${baseLinkPath}/browser`}
              activeClassName="active"
              onClick={this.handlePropagation.bind(this, {type: 'file'})}
              isNativeLink={this.props.singleWorkspaceMode}
            >
              <span className="fa fa-fw">
                <IconSVG name="icon-hdfs" />
              </span>

              <span>
                {T.translate(`${PREFIX}.hdfs`)}
              </span>
            </NavLinkWrapper>
          </div>

          <ExpandableMenu>
            <div>
              <span className="fa fa-fw">
                <IconSVG name="icon-database" />
              </span>
              <span>
              {T.translate(`${PREFIX}.database`, {count: this.state.databaseList.length})}
              </span>
            </div>
            {this.renderDatabaseDetail()}
          </ExpandableMenu>

          <ExpandableMenu>
            <div>
              <span className="fa fa-fw">
                <IconSVG name="icon-kafka" />
              </span>
              <span>
              {T.translate(`${PREFIX}.kafka`, {count: this.state.kafkaList.length})}
              </span>
            </div>
            {this.renderKafkaDetail()}
          </ExpandableMenu>

          <ExpandableMenu>
            <div>
              <span className="fa fa-fw">
                <IconSVG name="icon-s3" />
              </span>
              <span>
              {T.translate(`${PREFIX}.s3`, {count: this.state.s3List.length})}
              </span>
            </div>
            {this.renderS3Detail()}
          </ExpandableMenu>

          <ExpandableMenu>
            <div>
              <span className="fa fa-fw">
                <IconSVG name="icon-storage" />
              </span>
              <span>
              {T.translate(`${PREFIX}.gcs`, {count: this.state.gcsList.length})}
              </span>
            </div>
            {this.renderGCSDetail()}
          </ExpandableMenu>

          <ExpandableMenu>
            <div>
              <span className="fa fa-fw">
                <IconSVG name="icon-bigquery" />
              </span>
              <span>
              {T.translate(`${PREFIX}.bigquery`, {count: this.state.bigQueryList.length})}
              </span>
            </div>
            {this.renderBigQueryDetail()}
          </ExpandableMenu>
        </div>

        <AddConnection
          onAdd={this.fetchConnectionsList}
        />
      </div>
    );
  }

  renderRoutes() {
    const BASEPATH = '/ns/:namespace/connections';
    return (
      <Switch>
        <Route
          path={`${BASEPATH}/browser`}
          render={({match, location}) => {
            const setBrowserToDisplay = setActiveBrowser.bind(null, {name: 'file'});
            return (
              <DataPrepBrowser
                match={match}
                location={location}
                toggle={this.toggleSidePanel}
                onWorkspaceCreate={this.onUploadSuccess}
                setActiveBrowser={setBrowserToDisplay}
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
            let id  = match.match.params.databaseId;
            const setBrowserToDisplay = setDatabaseAsActiveBrowser.bind(null, {name: 'database', id});
            return (
              <DataPrepBrowser
                match={match}
                location={location}
                toggle={this.toggleSidePanel}
                onWorkspaceCreate={this.onUploadSuccess}
                setActiveBrowser={setBrowserToDisplay}
              />
            );
          }}
        />
        <Route
          path={`${BASEPATH}/kafka/:kafkaId`}
          render={(match) => {
            let id  = match.match.params.kafkaId;
            const setBrowserToDisplay = setKafkaAsActiveBrowser.bind(null, {name: 'kafka', id});
            return (
              <DataPrepBrowser
                match={match}
                location={location}
                toggle={this.toggleSidePanel}
                onWorkspaceCreate={this.onUploadSuccess}
                setActiveBrowser={setBrowserToDisplay}
              />
            );
          }}
        />
        <Route
          path={`${BASEPATH}/s3/:s3Id`}
          render={(match) => {
            let id  = match.match.params.s3Id;
            let {prefix = '/'} = queryString.parse(match.location.search);
            const setBrowserToDisplay = setS3AsActiveBrowser.bind(null, {name: 's3', id, path: prefix});
            return (
              <DataPrepBrowser
                match={match}
                location={location}
                toggle={this.toggleSidePanel}
                onWorkspaceCreate={this.onUploadSuccess}
                setActiveBrowser={setBrowserToDisplay}
              />
            );
          }}
        />
        <Route
          path={`${BASEPATH}/gcs/:gcsId`}
          render={(match) => {
            let id  = match.match.params.gcsId;
            let {prefix = '/'} = queryString.parse(match.location.search);
            const setBrowserToDisplay = setGCSAsActiveBrowser.bind(null, {name: 'gcs', id, path: prefix});
            return (
              <DataPrepBrowser
                match={match}
                location={location}
                toggle={this.toggleSidePanel}
                onWorkspaceCreate={this.onUploadSuccess}
                setActiveBrowser={setBrowserToDisplay}
              />
            );
          }}
        />
        <Route
          path={`${BASEPATH}/bigquery/:bigQueryId`}
          render={(match) => {
            let id  = match.match.params.bigQueryId;
            const setBrowserToDisplay = setBigQueryAsActiveBrowser.bind(null, {name: 'bigquery', id});
            return (
              <DataPrepBrowser
                match={match}
                location={location}
                toggle={this.toggleSidePanel}
                onWorkspaceCreate={this.onUploadSuccess}
                setActiveBrowser={setBrowserToDisplay}
              />
            );
          }}
        />
        <Route component={RouteToHDFS} />
      </Switch>
    );
  }
  showNonRoutableContents() {
    if (this.state.showUpload) {
      return (
        <ConnectionsUpload
          toggle={this.toggleSidePanel}
          onWorkspaceCreate={this.onUploadSuccess}
        />
      );
    }
    let {enableRouting, ...attributes} = this.props;
    enableRouting = this.props.singleWorkspaceMode ? false : this.props.enableRouting;
    let setBrowserToDisplay;
    if (this.state.activeConnectionType === 'database') {
      setBrowserToDisplay = setDatabaseAsActiveBrowser.bind(null, {name: 'database', id: this.state.activeConnectionid});
    } else if (this.state.activeConnectionType === 'kafka') {
      setBrowserToDisplay = setKafkaAsActiveBrowser.bind(null, {name: 'kafka', id: this.state.activeConnectionid});
    } else if (this.state.activeConnectionType === 'file') {
      setBrowserToDisplay = setActiveBrowser.bind(null, {name: 'file'});
    } else if (this.state.activeConnectionType === 's3') {
      let {workspaceInfo} = DataPrepStore.getState().dataprep;
      let {key} = workspaceInfo.properties;
      let bucketName = workspaceInfo.properties['bucket-name'];
      let path;
      if (bucketName) {
        path = `/${bucketName}/${key}`;
      }
      setBrowserToDisplay = setS3AsActiveBrowser.bind(null, {name: 's3', id: this.state.activeConnectionid, path});
    } else if (this.state.activeConnectionType === 'gcs') {
      let {workspaceInfo} = DataPrepStore.getState().dataprep;
      let {path} = workspaceInfo.properties;
      path = path.split('/');
      path = path.slice(0, path.length - 1).join('/');
      let bucketName = workspaceInfo.properties.bucket;
      if (bucketName) {
        path = `/${bucketName}/${path}/`;
      }
      setBrowserToDisplay = setGCSAsActiveBrowser.bind(null, {name: 'gcs', id: this.state.activeConnectionid, path});
    } else if (this.state.activeConnectionType === 'bigquery') {
      let {workspaceInfo} = DataPrepStore.getState().dataprep;
      setBrowserToDisplay = () => {
        setBigQueryAsActiveBrowser({name: 'bigquery', id: this.state.activeConnectionid});
        listBigQueryTables(workspaceInfo.properties.connectionid, workspaceInfo.properties.datasetId);
      };
    }
    return (
      <DataPrepBrowser
        match={this.props.match}
        location={this.props.location}
        toggle={this.toggleSidePanel}
        onWorkspaceCreate={!this.props.singleWorkspaceMode ? null : this.props.onWorkspaceCreate}
        enableRouting={enableRouting}
        setActiveBrowser={setBrowserToDisplay}
        {...attributes}
      />
    );
  }

  render() {
    let pageTitle = (
      <Helmet
        title={T.translate(DATAPREP_I18N_PREFIX)}
      />
    );
    if (this.state.backendChecking) {
      return (
        <div className="text-xs-center">
          {this.props.singleWorkspaceMode || this.props.enableRouting ? null : pageTitle}
          <LoadingSVG />
        </div>
      );
    }

    if (this.state.backendDown) {
      return (
        <div>
          {this.props.singleWorkspaceMode || this.props.enableRouting ? null : pageTitle}
          <DataPrepServiceControl
            onServiceStart={this.onServiceStart}
          />
        </div>
      );
    }

    let {backendChecking, loading} = this.state;
    if (backendChecking || loading) {
      return (<LoadingSVGCentered />);
    }
    return (
      <div className="dataprep-connections-container">
        {
          this.props.enableRouting ?
            <Helmet title={T.translate(`${PREFIX}.pageTitle`)} />
          :
            null
        }
        {this.props.singleWorkspaceMode || this.props.enableRouting ? null : pageTitle}
        {this.renderPanel()}

        <div className={classnames('connections-content', {
          'expanded': !this.state.sidePanelExpanded
        })}>
          {
            this.props.enableRouting && !this.props.singleWorkspaceMode ?
              this.renderRoutes()
            :
              this.showNonRoutableContents()
          }
        </div>

      </div>
    );
  }
}

DataPrepConnections.defaultProps = {
  enableRouting: true,
  allowSidePanelToggle: true
};

DataPrepConnections.propTypes = {
  match: PropTypes.object,
  location: PropTypes.object,
  enableRouting: PropTypes.bool,
  onWorkspaceCreate: PropTypes.func,
  singleWorkspaceMode: PropTypes.bool,
  sidePanelExpanded: PropTypes.bool,
  allowSidePanelToggle: PropTypes.bool,
  scope: PropTypes.oneOfType([PropTypes.bool, PropTypes.string]),
  browserTitle: PropTypes.string
};
