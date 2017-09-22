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
import { Nav, NavItem, NavLink, TabContent, TabPane} from 'reactstrap';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
require('./ViewSwitch.scss');
import cookie from 'react-cookie';

export default class ViewSwitch extends Component {
  constructor(props) {
    super(props);
    let defaultView = cookie.load('ViewSwitchDefault');
    this.state = {
      list: this.props.list,
      activeTab: defaultView || 'card'
    };
  }
  toggleView(tab) {
    if (this.state.activeTab !== tab) {
      cookie.save('ViewSwitchDefault', tab);
      this.setState({
        activeTab: tab
      });
      if (this.props.onSwitch) {
        this.props.onSwitch();
      }
    }
  }
  render() {
    return (
      <div className="view-switch">
        <div className="clearfix">
          <Nav className="float-xs-right" tabs>
            <NavItem>
              <NavLink
                className={classnames({ active: this.state.activeTab === 'card' })}
                onClick={() => {this.toggleView('card');}}>
                <IconSVG name="icon-th-large" />
              </NavLink>
            </NavItem>
            <NavItem>
              <NavLink
                className={classnames({ active: this.state.activeTab === 'list' })}
                onClick={() => {this.toggleView('list');}}>
                <IconSVG name="icon-list" />
              </NavLink>
            </NavItem>
          </Nav>
        </div>
        <TabContent activeTab={this.state.activeTab}>
          <TabPane tabId="card">
            { this.state.activeTab === 'card' ? this.props.children[0] : null}
          </TabPane>
          <TabPane tabId="list">
            { this.state.activeTab === 'list' ? this.props.children[1] : null}
          </TabPane>
        </TabContent>
      </div>
    );
  }
}
ViewSwitch.propTypes = {
  list: PropTypes.arrayOf(PropTypes.object),
  children: PropTypes.node,
  onSwitch: PropTypes.func
};
