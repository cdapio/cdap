/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {Component} from 'react';
import { Dropdown, DropdownMenu, DropdownItem } from 'reactstrap';
import cookie from 'react-cookie';

require('./HeaderActions.less');
var classNames = require('classnames');
import PlusButton from '../PlusButton';

export default class HeaderActions extends Component {

  constructor(props) {
    super(props);
    this.state = {
      settingsOpen : false,
      name : cookie.load('CDAP_Auth_User')
    };
    this.logout = this.logout.bind(this);
    this.toggleDropdown = this.toggleDropdown.bind(this);
  }

  toggleDropdown(){
    this.setState({
      settingsOpen : !this.state.settingsOpen
    });
  }

  logout() {
    cookie.remove('CDAP_Auth_User');
    this.setState({
      name : ''
    });
  }

  render() {

    let topRow = '';
    let signoutRow = '';

    if(this.state.name){
      topRow = (
        <div>
          <div className="dropdown-item dropdown-name-row">
            Signed in as <span className="dropdown-name">{this.state.name}</span> <span className="dropdown-name-icon fa fa-cog"></span>
          </div>
          <DropdownItem divider />
        </div>
      );

      signoutRow = (
        <div>
          <DropdownItem divider />
          <div className="dropdown-item" onClick={this.logout}><span className="dropdown-icon fa fa-sign-out"></span>Logout</div>
        </div>
      );
    }
    return (
      <div className="header-actions">
        <ul className="navbar-list pull-right">
          <div className="navbar-item">
            <span className="fa fa-search"></span>
          </div>
          <div className="navbar-item">
            <span className="fa fa-bolt"></span>
          </div>
          <PlusButton className="navbar-item" />
          <div
            className="navbar-item navbar-cog"
            onClick={this.toggleDropdown.bind(this)}
          >
            <span className={classNames('fa', 'fa-cog', {'menu-open' : this.state.settingsOpen})}></span>
            <span className={classNames('navbar-cog-arrow', {'hidden' : !this.state.settingsOpen})}></span>
            <Dropdown isOpen={this.state.settingsOpen} toggle={this.toggleDropdown.bind(this)}>
              <DropdownMenu>
                {topRow}
                <div className="dropdown-item"><span className="dropdown-icon fa fa-life-ring"></span>Support</div>
                <div className="dropdown-item"><span className="dropdown-icon fa fa-home"></span>Cask home</div>
                <div className="dropdown-item"><span className="dropdown-icon fa fa-file"></span>Documentation</div>
                {signoutRow}
              </DropdownMenu>
            </Dropdown>
          </div>
          <div className="navbar-item namespace-dropdown dropdown">
            <span> Namespace </span>
          </div>
        </ul>
      </div>
    );
  }
}
