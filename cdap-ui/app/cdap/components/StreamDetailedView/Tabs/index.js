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
import { Nav, NavItem, TabContent} from 'reactstrap';
import isNil from 'lodash/isNil';
import {Route, NavLink as RouterNavLink} from 'react-router-dom';
import ProgramTab from 'components/Overview/Tabs/ProgramTab';
import SchemaTab from 'components/Overview/Tabs/SchemaTab';
import UsageTab from 'components/StreamDetailedView/Tabs/UsageTab';
import AuditTab from 'components/StreamDetailedView/Tabs/AuditTab';
import LineageTab from 'components/StreamDetailedView/Tabs/LineageTab';
import PropertiesTab from 'components/StreamDetailedView/Tabs/PropertiesTab';
import T from 'i18n-react';

const PREFIX = 'features.DatasetDetailedView.Tabs';

export default class StreamDetailedViewTabs extends Component {
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
    const baseLinkPath = `/ns/${this.props.params.namespace}/streams/${this.props.params.streamId}`;
    const baseMatchPath = `/ns/:namespace/streams/:streamId`;

    return (
      <div className="overview-tab">
        <Nav tabs>
          <NavItem>
            <div className="nav-link">
              <RouterNavLink
                to={`${baseLinkPath}/usage`}
                activeClassName="active"
                isActive={(match, location) => {
                  let basepath = `^${baseLinkPath}(/usage)?$`;
                   return location.pathname.match(basepath);
                }}
              >
              {T.translate(`${PREFIX}.usage`)}
              </RouterNavLink>
            </div>
          </NavItem>

          <NavItem>
            <div className="nav-link">
              <RouterNavLink
                to={`${baseLinkPath}/schema`}
                activeClassName="active"
              >
                {T.translate(`${PREFIX}.schema`)}
              </RouterNavLink>
            </div>
          </NavItem>

          <NavItem>
            <div className="nav-link">
              <RouterNavLink
                to={`${baseLinkPath}/programs`}
                activeClassName="active"
              >
                {T.translate(`${PREFIX}.programsWithCount`, {
                  count: this.state.entity.programs.length
                })}
              </RouterNavLink>
            </div>
          </NavItem>

          <NavItem>
            <div className="nav-link">
              <RouterNavLink
                to={`${baseLinkPath}/lineage`}
                activeClassName="active"
              >
                {T.translate(`${PREFIX}.lineage`)}
              </RouterNavLink>
            </div>
          </NavItem>

          <NavItem>
            <div className="nav-link">
              <RouterNavLink
                to={`${baseLinkPath}/audit`}
                activeClassName="active"
              >
                {T.translate(`${PREFIX}.auditLog`)}
              </RouterNavLink>
            </div>
          </NavItem>

          <NavItem>
            <div className="nav-link">
              <RouterNavLink
                to={`${baseLinkPath}/properties`}
                activeClassName="active"
              >
                {T.translate(`${PREFIX}.properties`)}
              </RouterNavLink>
            </div>
          </NavItem>
        </Nav>
        <TabContent>
          <Route exact path={`${baseMatchPath}/`} render={
            () => {
              return (
                <UsageTab entity={this.state.entity} />
              );
            }}
          />
        <Route exact path={`${baseMatchPath}/usage`} render={
            () => {
              return (
                <UsageTab entity={this.state.entity} />
              );
            }}
          />
          <Route path={`${baseMatchPath}/schema`} render={
            () => {
              return (
                <SchemaTab entity={this.state.entity} />
              );
            }}
          />
        <Route exact path={`${baseMatchPath}/programs`} render={
            () => {
              return (
                <ProgramTab entity={this.state.entity} />
              );
            }}
          />
        <Route exact path={`${baseMatchPath}/lineage`} render={
            () => {
              return (
                <LineageTab entity={this.state.entity} />
              );
            }}
          />
        <Route exact path={`${baseMatchPath}/audit`} render={
            () => {
              return (
                <AuditTab entity={this.state.entity} />
              );
            }}
          />
        <Route exact path={`${baseMatchPath}/properties`} render={
            () => {
              return (
                <PropertiesTab entity={this.state.entity} />
              );
            }}
          />
        </TabContent>
      </div>
    );
  }
}

StreamDetailedViewTabs.propTypes = {
  entity: PropTypes.object,
  location: PropTypes.string,
  pathname: PropTypes.string,
  params: PropTypes.object
};
