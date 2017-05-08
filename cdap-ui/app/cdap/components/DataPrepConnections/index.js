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
import FileBrowser from 'components/FileBrowser';
import NamespaceStore from 'services/NamespaceStore';
import T from 'i18n-react';

require('./DataPrepConnections.scss');
const PREFIX = 'features.DataPrepConnections';

const RouteToHDFS = () => {
  let namespace = NamespaceStore.getState().selectedNamespace;

  return (
    <Redirect to={`/ns/${namespace}/connections/browser`} />
  );
};

const UploadPlaceholder = () => {
  return (
    <div>
      <h3 className="text-xs-center">UPLOAD</h3>
    </div>
  );
};

export default class DataPrepConnections extends Component {
  constructor(props) {
    super(props);

    this.state = {
      sidePanelExpanded: true
    };

    this.toggleSidePanel = this.toggleSidePanel.bind(this);
  }

  toggleSidePanel() {
    this.setState({sidePanelExpanded: !this.state.sidePanelExpanded});
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
        </div>
      </div>
    );
  }

  render() {
    const BASEPATH = '/ns/:namespace/connections';

    return (
      <div className="dataprep-connections-container">
        {this.renderPanel()}

        <div className={classnames('connections-content', {
          'expanded': !this.state.sidePanelExpanded
        })}>
          <Route path={`${BASEPATH}/browser`}
            render={({match, location}) => {
              return (
                <FileBrowser
                  match={match}
                  location={location}
                  toggle={this.toggleSidePanel}
                />
              );
            }}
          />
          <Route path={`${BASEPATH}/upload`} component={UploadPlaceholder} />
        </div>

        <Route exact path={`${BASEPATH}`} component={RouteToHDFS} />
      </div>
    );
  }
}
