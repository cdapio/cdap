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
import {Link} from 'react-router';
require('./Header.less');

export default class Header extends Component {
  constructor(props) {
    super(props);
    this.props = props;
    this.state = {
      showSidebar: false
    };
  }
  getAbsUrl(extension) {
    switch(extension) {
      case 'hydrator':
        return window.getAbsUIUrl({
          uiApp: 'hydrator'
        });
      case 'tracker':
        return window.getAbsUIUrl({
          uiApp: 'tracker'
        });
      default:
        return window.getAbsUIUrl();
    }
  }
  sidebarClickNoOp(e) {
    e.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
    return false;
  }
  toggleSidebar() {
    this.setState({showSidebar: !this.state.showSidebar});
  }
  render() {
    return (
      <CDAP-Header>
        <header className="navbar navbar-fixed-top">
          <nav className="navbar cdap">
            <div className="brand-header">
              <a className="navbar-brand"
                 onClick={this.toggleSidebar.bind(this)}>
                <span className="fa icon-fist"></span>
              </a>
              <a href="/" className="menu-item product-title">
                CDAP
              </a>
            </div>
            <ul className="navbar-list">
              <li>
                <Link to="home" activeClassName="active">
                  Home
                </Link>
              </li>
              <li>
                <Link to="dashboard" activeClassName="active">
                  Dashboard
                </Link>
              </li>
              <li>
                <Link to="management" activeClassName="active">
                  Managment
                </Link>
              </li>
            </ul>
          </nav>
        </header>
        <div className={this.state.showSidebar ? 'display-container': 'hide'}
             onClick={this.toggleSidebar.bind(this)}>
          <div className="sidebar" onClick={this.sidebarClickNoOp.bind(this)}>
            <a href="/"
               className="brand sidebar-item top">
              <div className="brand-icon text-center cdap">
                <span className="icon-fist"></span>
              </div>

              <div className="product-name">
                <span>CDAP</span>
              </div>
            </a>

            <h5>Extensions:</h5>
            {
            /*
              FIXME: This should be dynamic; based on host, port & other contextual information.
              Need to build a utility function ASAP
            */
            }
            <a href={this.getAbsUrl('hydrator')}
               className="brand sidebar-item">
              <div className="brand-icon text-center hydrator">
                <span className="icon-hydrator"></span>
              </div>

              <div className="product-name">
                <span>Cask Hydrator</span>
              </div>
            </a>

            <a href={this.getAbsUrl('tracker')}
               className="brand sidebar-item">
              <div className="brand-icon text-center tracker">
                <span className="icon-tracker"></span>
              </div>

              <div className="product-name">
                <span>Cask Tracker</span>
              </div>
            </a>
          </div>
        </div>
      </CDAP-Header>
    );
  }
}
