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
import isNil from 'lodash/isNil';
import {Route, Switch, NavLink as RouterNavLink} from 'react-router-dom';
import ProgramTab from 'components/Overview/Tabs/ProgramTab';
import SchemaTab from 'components/Overview/Tabs/SchemaTab';
import UsageTab from 'components/DatasetDetailedView/Tabs/UsageTab';
import AuditTab from 'components/DatasetDetailedView/Tabs/AuditTab';
import LineageTab from 'components/DatasetDetailedView/Tabs/LineageTab';
import PropertiesTab from 'components/DatasetDetailedView/Tabs/PropertiesTab';

export default class DatasetDetailedViewTabs extends Component {
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
    const baseLinkPath = `/ns/${this.props.params.namespace}/datasets/${this.props.params.datasetId}`;
    const baseMatchPath = `/ns/:namespace/datasets/:datasetId`;

    return (
      <div className="overview-tab">
        <Nav tabs>
          <NavItem>
            <NavLink>
              <RouterNavLink
                to={`${baseLinkPath}/usage`}
                activeClassName="active"
                isActive={(match, location) => {
                  let basepath = `^${baseLinkPath}(/usage)?$`;
                   return location.pathname.match(basepath);
                }}
              >
                Usage
              </RouterNavLink>
            </NavLink>
          </NavItem>

          <NavItem>
            <NavLink>
              <RouterNavLink
                to={`${baseLinkPath}/schema`}
                activeClassName="active"
              >
                Schema
              </RouterNavLink>
            </NavLink>
          </NavItem>

          <NavItem>
            <NavLink>
              <RouterNavLink
                to={`${baseLinkPath}/programs`}
                activeClassName="active"
              >
                Programs ({this.state.entity.programs.length})
              </RouterNavLink>
            </NavLink>
          </NavItem>

          <NavItem>
            <NavLink>
              <RouterNavLink
                to={`${baseLinkPath}/lineage`}
                activeClassName="active"
              >
                Lineage
              </RouterNavLink>
            </NavLink>
          </NavItem>

          <NavItem>
            <NavLink>
              <RouterNavLink
                to={`${baseLinkPath}/audit`}
                activeClassName="active"
              >
                Audit Log
              </RouterNavLink>
            </NavLink>
          </NavItem>

          <NavItem>
            <NavLink>
              <RouterNavLink
                to={`${baseLinkPath}/properties`}
                activeClassName="active"
              >
                Properties
              </RouterNavLink>
            </NavLink>
          </NavItem>
        </Nav>
        <TabContent>
          <Switch>
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
            <Route exact path={`${baseMatchPath}/schema`} render={
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
          </Switch>
        </TabContent>
      </div>
    );
  }
}

DatasetDetailedViewTabs.propTypes = {
  entity: PropTypes.object,
  params: PropTypes.shape({
    datasetId: PropTypes.string,
    namespace: PropTypes.string
  }),
  pathname: PropTypes.string
};
