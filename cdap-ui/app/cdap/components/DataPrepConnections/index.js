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
import IconSVG from 'components/IconSVG';
import classnames from 'classnames';
import {Route, NavLink, Redirect} from 'react-router-dom';
import DataPrepBrowser from 'components/DataPrep/DataPrepBrowser';
import {setActiveBrowser} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import NamespaceStore from 'services/NamespaceStore';
import T from 'i18n-react';
import LoadingSVG from 'components/LoadingSVG';
import MyDataPrepApi from 'api/dataprep';
import DataPrepServiceControl from 'components/DataPrep/DataPrepServiceControl';
import ConnectionsUpload from 'components/DataPrepConnections/UploadFile';
import AddConnection from 'components/DataPrepConnections/AddConnection';
import DatabaseBrowserWrapper from 'components/DataPrepConnections/DatabaseBrowserWrapper';

require('./DataPrepConnections.scss');
const PREFIX = 'features.DataPrepConnections';

const RouteToHDFS = () => {
  let namespace = NamespaceStore.getState().selectedNamespace;

  return (
    <Redirect to={`/ns/${namespace}/connections/browser`} />
  );
};

export default class DataPrepConnections extends Component {
  constructor(props) {
    super(props);

    this.state = {
      sidePanelExpanded: true,
      backendChecking: true,
      backendDown: false,
      connectionsList: [],
      database: false
    };

    this.toggleSidePanel = this.toggleSidePanel.bind(this);
    this.onServiceStart = this.onServiceStart.bind(this);
    this.fetchConnectionsList = this.fetchConnectionsList.bind(this);
  }

  componentWillMount() {
    this.checkBackendUp();
    // FIXME: This is temporary. Based on standalone or cluster we will have this default
    // to different things. For now it defaults to browser.
    setActiveBrowser({name: 'file'});
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

  onServiceStart() {
    this.checkBackendUp();
  }

  fetchConnectionsList() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    MyDataPrepApi.listConnections({
      namespace,
      type: '*'
    }).subscribe((res) => {
      // need to group by connection type

      this.setState({connectionsList: res.values});
    });
  }

  toggleSidePanel() {
    this.setState({sidePanelExpanded: !this.state.sidePanelExpanded});
  }

  renderDatabaseDetail() {
    if (!this.state.database) { return null; }

    let namespace = NamespaceStore.getState().selectedNamespace;
    const baseLinkPath = `/ns/${namespace}/connections`;

    return (
      <div>
        {this.state.connectionsList.map((database) => {
          return (
            <NavLink
              to={`${baseLinkPath}/database/${database.id}`}
              activeClassName="active"
              className="menu-item-expanded-list"
              key={database.id}
            >
              {database.name}
            </NavLink>
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
            <NavLink
              to={`${baseLinkPath}/upload`}
              activeClassName="active"
            >
              <span className="fa fa-fw">
                <IconSVG name="icon-upload" />
              </span>

              <span>
                {T.translate(`${PREFIX}.upload`)}
              </span>
            </NavLink>
          </div>

          <div className="menu-item">
            <NavLink
              to={`${baseLinkPath}/browser`}
              activeClassName="active"
            >
              <span className="fa fa-fw">
                <IconSVG name="icon-hdfs" />
              </span>

              <span>
                {T.translate(`${PREFIX}.hdfs`)}
              </span>
            </NavLink>
          </div>

          <div className="menu-item expandable">
            <div
              className="expandable-title"
              onClick={() => this.setState({database: !this.state.database})}
            >
              <span className="fa fa-fw caret">
                <IconSVG
                  name={this.state.database ? 'icon-caret-down' : 'icon-caret-right'}
                />
              </span>
              <span className="fa fa-fw">
                <IconSVG name="icon-database" />
              </span>
              <span>
              {T.translate(`${PREFIX}.database`, {count: this.state.connectionsList.length})}
              </span>
            </div>

            {this.renderDatabaseDetail()}
          </div>
        </div>

        <AddConnection
          onAdd={this.fetchConnectionsList}
        />
      </div>
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


    const BASEPATH = '/ns/:namespace/connections';

    return (
      <div className="dataprep-connections-container">
        {this.renderPanel()}

        <div className={classnames('connections-content', {
          'expanded': !this.state.sidePanelExpanded
        })}>
          <Route path={`${BASEPATH}/browser`}
            render={({match, location}) => {
              setActiveBrowser({ name: 'file' });

              return (
                <DataPrepBrowser
                  match={match}
                  location={location}
                  toggle={this.toggleSidePanel}
                />
              );
            }}
          />
          <Route path={`${BASEPATH}/upload`}
            render={() => {
              return (
                <ConnectionsUpload toggle={this.toggleSidePanel} />
              );
            }}
          />

          <Route
            path={`${BASEPATH}/database/:databaseId`}
            render={(match) => {
              return (
                <DatabaseBrowserWrapper
                  databaseId={match.match.params.databaseId}
                />
              );
            }}
          />
        </div>

        <Route exact path={`${BASEPATH}`} component={RouteToHDFS} />
      </div>
    );
  }
}
