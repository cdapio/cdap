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
import { Nav, NavItem, NavLink} from 'reactstrap';
import isNil from 'lodash/isNil';
import Match from 'react-router/Match';
import Link from 'react-router/Link';
import ProgramTab from 'components/Overview/Tabs/ProgramTab';
import SchemaTab from 'components/Overview/Tabs/SchemaTab';

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
            <NavLink>
              <Link
                to={`${baseLinkPath}/programs`}
                activeClassName="active"
                isActive={(location) => {
                  let basepath = `^${baseLinkPath}(/programs)?$`;
                   return location.pathname.match(basepath);
                }}
              >
                Programs ({this.state.entity.programs.length})
              </Link>
            </NavLink>
          </NavItem>

          <NavItem>
            <NavLink>
              <Link
                to={`${baseLinkPath}/schema`}
                activeClassName="active"
              >
                Schema
              </Link>
            </NavLink>
          </NavItem>
        </Nav>
        <Match pattern={`${baseMatchPath}/`} render={
          () => {
            return (
              <ProgramTab entity={this.state.entity} />
            );
          }}
        />
        <Match pattern={`${baseMatchPath}/programs`} render={
          () => {
            return (
              <ProgramTab entity={this.state.entity} />
            );
          }}
        />
        <Match pattern={`${baseMatchPath}/schema`} render={
          () => {
            return (
              <SchemaTab entity={this.state.entity} />
            );
          }}
        />
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

