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
import IconSVG from 'components/IconSVG';
import classnames from 'classnames';
import DataPrepBrowser from 'components/DataPrep/DataPrepBrowser';
import {
  setActiveBrowser,
  setDatabaseAsActiveBrowser,
  setKafkaAsActiveBrowser
} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import {Route, NavLink, Redirect, Switch} from 'react-router-dom';
import NamespaceStore from 'services/NamespaceStore';
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

require('./DataPrepConnections.scss');
const PREFIX = 'features.DataPrepConnections';

const RouteToHDFS = () => {
  let namespace = NamespaceStore.getState().selectedNamespace;

  return (
    <Redirect to={`/ns/${namespace}/connections/browser`} />
  );
};
const NavLinkWrapper = ({children, to, singleWorkspaceMode, ...attributes}) => {
  if (singleWorkspaceMode) {
    return (
      <a
        href={to}
        {...attributes}
      >
        {children}
      </a>
    );
  }
  return (
    <NavLink
      to={to}
      {...attributes}
    >
      {children}
    </NavLink>
  );
};

NavLinkWrapper.propTypes = {
  children: PropTypes.node,
  to: PropTypes.string,
  singleWorkspaceMode: PropTypes.bool
};

export default class DataPrepConnections extends Component {
  constructor(props) {
    super(props);

    let {workspaceInfo} = DataPrepStore.getState().dataprep;
    this.state = {
      sidePanelExpanded: this.props.enableRouting ? true : false,
      backendChecking: true,
      backendDown: false,
      databaseList: [],
      kafkaList: [],
      activeConnectionid: objectQuery(workspaceInfo, 'properties', 'connectionid'),
      activeConnectionType: objectQuery(workspaceInfo, 'properties', 'connection'),
      showUpload: false // FIXME: This is used only when showing with no routing. We can do better.
    };

    this.toggleSidePanel = this.toggleSidePanel.bind(this);
    this.onServiceStart = this.onServiceStart.bind(this);
    this.fetchConnectionsList = this.fetchConnectionsList.bind(this);
    this.onUploadSuccess = this.onUploadSuccess.bind(this);

  }

  componentWillMount() {
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
    if (this.dataprepSubscription) {
      this.dataprepSubscription();
    }
  }
  checkBackendUp() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    MyDataPrepApi.ping({ namespace })
      .subscribe(() => {
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
      setActiveBrowser({name: browserName});
      return;
    }
    preventPropagation(e);

    if (isNil(browserName)) {
      return;
    }

    // FIXME: This feels adhoc. We should be able to simplify this.
    if (browserName === 'upload') {
      this.setState({
        showUpload: true
      });
      return;
    }
    let activeConnectionType, activeConnectionid;
    if (browserName === 'file') {
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
    }

    this.setState({
      showUpload: false,
      activeConnectionType,
      activeConnectionid
    });
  }

  onServiceStart() {
    this.checkBackendUp();
  }

  fetchConnectionsList(action, targetId) {
    let namespace = NamespaceStore.getState().selectedNamespace;

    MyDataPrepApi.listConnections({
      namespace,
      type: '*' // currently only going to fetch database connection
    }).subscribe((res) => {
      // need to group by connection type

      let databaseList = [],
          kafkaList = [];

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
        }
      });

      state.databaseList = databaseList;
      state.kafkaList = kafkaList;
      this.setState(state);
    });
  }

  toggleSidePanel() {
    this.setState({sidePanelExpanded: !this.state.sidePanelExpanded});
  }

  onUploadSuccess(workspaceId) {
    if (this.props.enableRouting) {
      let {selectedNamespace: namespace} = NamespaceStore.getState();
      let navigatePath = `${window.location.origin}/cdap/ns/${namespace}/dataprep/${workspaceId}`;
      window.location.href = navigatePath;
      return;
    }
    if (this.props.onWorkspaceCreate) {
      this.props.onWorkspaceCreate(workspaceId);
    }
  }
  renderDatabaseDetail() {
    let namespace = NamespaceStore.getState().selectedNamespace;
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
                singleWorkspaceMode={this.props.singleWorkspaceMode}
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
    let namespace = NamespaceStore.getState().selectedNamespace;
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
                onClick={this.handlePropagation.bind(this, kafka)}
                singleWorkspaceMode={this.props.singleWorkspaceMode}
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

  renderPanel() {
    if (!this.state.sidePanelExpanded)  { return null; }

    let namespace = NamespaceStore.getState().selectedNamespace;
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
              onClick={this.handlePropagation.bind(this, 'upload')}
              singleWorkspaceMode={this.props.singleWorkspaceMode}
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
              onClick={this.handlePropagation.bind(this, 'file')}
              singleWorkspaceMode={this.props.singleWorkspaceMode}
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
            setActiveBrowser({name: 'file'});
            return (
              <DataPrepBrowser
                match={match}
                location={location}
                toggle={this.toggleSidePanel}
                onWorkspaceCreate={this.onUploadSuccess}
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
            setDatabaseAsActiveBrowser({name: 'database', id});
            return (
              <DataPrepBrowser
                match={match}
                location={location}
                toggle={this.toggleSidePanel}
                onWorkspaceCreate={this.onUploadSuccess}
              />
            );
          }}
        />
        <Route
          path={`${BASEPATH}/kafka/:kafkaId`}
          render={(match) => {
            let id  = match.match.params.kafkaId;
            setKafkaAsActiveBrowser({name: 'kafka', id});
            return (
              <DataPrepBrowser
                match={match}
                location={location}
                toggle={this.toggleSidePanel}
                onWorkspaceCreate={this.onUploadSuccess}
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
    if (this.state.activeConnectionType === 'database') {
      setDatabaseAsActiveBrowser({name: 'database', id: this.state.activeConnectionid});
    } else if (this.state.activeConnectionType === 'kafka') {
      setKafkaAsActiveBrowser({name: 'kafka', id: this.state.activeConnectionid});
    } else if (this.state.activeConnectionType === 'file') {
      setActiveBrowser({name: 'file'});
    }
    return (
      <DataPrepBrowser
        match={this.props.match}
        location={this.props.location}
        toggle={this.toggleSidePanel}
        onWorkspaceCreate={!this.props.singleWorkspaceMode ? null : this.props.onWorkspaceCreate}
        enableRouting={enableRouting}
        {...attributes}
      />
    );
  }
  render() {
    if (this.state.backendChecking) {
      return (
        <div className="text-xs-center">
          <LoadingSVG />
        </div>
      );
    }

    if (this.state.backendDown) {
      return (
        <DataPrepServiceControl
          onServiceStart={this.onServiceStart}
        />
      );
    }

    return (
      <div className="dataprep-connections-container">
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
  enableRouting: true
};

DataPrepConnections.propTypes = {
  match: PropTypes.object,
  location: PropTypes.object,
  enableRouting: PropTypes.bool,
  onWorkspaceCreate: PropTypes.func,
  singleWorkspaceMode: PropTypes.bool
};
