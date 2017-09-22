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
import { Nav, NavItem, NavLink, TabContent} from 'reactstrap';
import ProgramTab from 'components/Overview/Tabs/ProgramTab';
import DatasetTab from 'components/Overview/Tabs/DatasetTab';
import HistoryTab from 'components/AppDetailedView/Tabs/HistoryTab';
import PropertiesTab from 'components/AppDetailedView/Tabs/PropertiesTab';
import isNil from 'lodash/isNil';
import {Route, Switch, NavLink as RouterNavLink} from 'react-router-dom';
import T from 'i18n-react';

export default class AppDetailedViewTab extends Component {
  constructor(props) {
    super(props);
    this.state = {
      entity: this.props.entity
    };
  }
  componentWillReceiveProps(nextProps) {
    if (!isNil(nextProps.entity)) {
      this.setState({
        entity: nextProps.entity
      });
    }
  }
  render() {
    return (
      <div className="overview-tab">
        <Nav tabs>
          <NavItem>
            <NavLink>
              <RouterNavLink
                to={`/ns/${this.props.params.namespace}/apps/${this.props.params.appId}/programs`}
                activeClassName="active"
                isActive={(match, location) => {
                  let basepath = `^/ns/${this.props.params.namespace}/apps/${this.props.params.appId}(/programs)?$`;
                   return location.pathname.match(basepath);
                }}
              >
                {T.translate('features.AppDetailedView.Tabs.programsLabel')} ({this.state.entity.programs.length})
              </RouterNavLink>
            </NavLink>
          </NavItem>
          <NavItem>
            <NavLink>
              <RouterNavLink
                to={`/ns/${this.props.params.namespace}/apps/${this.props.params.appId}/datasets`}
                activeClassName="active"
              >
                {T.translate('features.AppDetailedView.Tabs.datasetsLabel')} ({this.state.entity.datasets.length + this.state.entity.streams.length})
              </RouterNavLink>
            </NavLink>
          </NavItem>
          <NavItem>
            <NavLink>
              <RouterNavLink
                to={`/ns/${this.props.params.namespace}/apps/${this.props.params.appId}/history`}
                activeClassName="active"
              >
                {T.translate('features.AppDetailedView.Tabs.historyLabel')}
              </RouterNavLink>
            </NavLink>
          </NavItem>
          <NavItem>
            <NavLink>
              <RouterNavLink
                to={`/ns/${this.props.params.namespace}/apps/${this.props.params.appId}/properties`}
                activeClassName="active"
              >
                {T.translate('features.AppDetailedView.Tabs.propertiesLabel')}
              </RouterNavLink>
            </NavLink>
          </NavItem>
        </Nav>
        <TabContent>
          <Switch>
            <Route exact path={'/ns/:namespace/apps/:appId/'} render={() => {
                return (
                  <ProgramTab entity={this.state.entity} />
                );
              }} />
            <Route exact path={'/ns/:namespace/apps/:appId/programs'} render={() => {
                return (
                  <ProgramTab entity={this.state.entity} />
                );
              }} />
            <Route exact path={'/ns/:namespace/apps/:appId/datasets'} render={() => {
                return (
                  <DatasetTab entity={this.state.entity} />
                );
              }} />
            <Route exact path={'/ns/:namespace/apps/:appId/history'} render={() => {
                return (
                  <HistoryTab entity={this.state.entity} />
                );
            }}/>
            <Route exact path={'/ns/:namespace/apps/:appId/properties'} render={() => {
                return (
                  <PropertiesTab entity={this.state.entity} />
                );
            }}/>
          </Switch>
        </TabContent>
      </div>
    );
  }
}

AppDetailedViewTab.propTypes = {
  entity: PropTypes.object,
  location: PropTypes.string,
  pathname: PropTypes.string,
  params: PropTypes.object
};
