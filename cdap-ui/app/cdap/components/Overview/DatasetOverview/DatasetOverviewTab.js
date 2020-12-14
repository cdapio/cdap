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
import { TabContent, TabPane, Nav, NavItem, NavLink, Row, Col } from 'reactstrap';
import ProgramTab from 'components/Overview/Tabs/ProgramTab';
import SchemaTab from 'components/Overview/Tabs/SchemaTab/index.tsx';
import classnames from 'classnames';
import isNil from 'lodash/isNil';
require('../Tabs/OverviewTab.scss');

export default class DatasetOverviewTab extends Component {
  constructor(props) {
    super(props);
    this.state = {
      activeTab: '1',
      entity: this.props.entity,
    };
  }
  componentWillReceiveProps(nextProps) {
    if (!isNil(nextProps.entity)) {
      this.setState({
        entity: nextProps.entity,
      });
    }
  }
  toggleTab(tab) {
    if (this.state.activeTab !== tab) {
      this.setState({
        activeTab: tab,
      });
    }
  }
  render() {
    if (!isNil(this.state.entity)) {
      return (
        <div className="overview-tab">
          <Nav tabs>
            <NavItem>
              <NavLink
                className={classnames({ active: this.state.activeTab === '1' })}
                onClick={() => {
                  this.toggleTab('1');
                }}
              >
                Programs ({this.state.entity.programs.length})
              </NavLink>
            </NavItem>

            <NavItem>
              <NavLink
                className={classnames({ active: this.state.activeTab === '2' })}
                onClick={() => {
                  this.toggleTab('2');
                }}
              >
                Schema
              </NavLink>
            </NavItem>
          </Nav>
          <TabContent activeTab={this.state.activeTab}>
            <TabPane tabId="1">
              <Row>
                <Col sm="12">
                  {this.state.activeTab === '1' ? <ProgramTab entity={this.state.entity} /> : null}
                </Col>
              </Row>
            </TabPane>

            <TabPane tabId="2">
              <Row>
                <Col sm="12">
                  {this.state.activeTab === '2' ? <SchemaTab entity={this.state.entity} /> : null}
                </Col>
              </Row>
            </TabPane>
          </TabContent>
        </div>
      );
    }
    return null;
  }
}
DatasetOverviewTab.propTypes = {
  entity: PropTypes.object,
};
