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

import React, {Component, PropTypes} from 'react';
import { Dropdown, DropdownMenu, DropdownItem } from 'reactstrap';
import PlusButton from '../PlusButton';
import T from 'i18n-react';
import NamespaceStore from 'services/NamespaceStore';
import SpotlightSearch from 'components/SpotlightSearch';
require('./HeaderActions.less');
var classNames = require('classnames');
import NamespaceDropdown from 'components/NamespaceDropdown';
import ProductsDrawer from 'components/ProductsDrawer';
import RedirectToLogin from 'services/redirect-to-login';

export default class HeaderActions extends Component {
  constructor(props) {
    super(props);
    this.state = {
      settingsOpen : false,
      name : NamespaceStore.getState().username,
    };
    this.logout = this.logout.bind(this);
    this.toggleSettingsDropdown = this.toggleSettingsDropdown.bind(this);
  }
  logout() {
    RedirectToLogin({statusCode: 401});
  }
  toggleSettingsDropdown(){
    this.setState({
      settingsOpen : !this.state.settingsOpen
    });
  }
  render() {

    let topRow = '';
    let signoutRow = '';

    if(this.state.name && window.CDAP_CONFIG.securityEnabled){
      topRow = (
        <div>
          <div className="dropdown-item dropdown-name-row">
            <span>{T.translate('features.Navbar.HeaderActions.signedInAs')}</span>
            <span className="dropdown-name">
              {this.state.name}
            </span>
          </div>
          <DropdownItem divider />
        </div>
      );

      signoutRow = (
        <div>
          <DropdownItem divider />
          <div
            className="dropdown-item"
            onClick={this.logout}
          >
            <span className="dropdown-icon fa fa-sign-out"></span>
            {T.translate('features.Navbar.HeaderActions.logout')}
          </div>
        </div>
      );
    }

    return (
      <div className="header-actions">
        <ul className="navbar-list pull-right">
          <div className="navbar-item">
            <SpotlightSearch />
          </div>
          {
            // FIXME: Add this later.
            // <div className="navbar-item">
            //   <span className="fa fa-bell"></span>
            // </div>
          }
          <PlusButton className="navbar-item" />
          <div
            className="navbar-item settings-dropdown navbar-cog"
            onClick={this.toggleSettingsDropdown}
          >
            <span
              className={classNames('fa', 'fa-cog', {'menu-open' : this.state.settingsOpen})}
            />
            <span
              className={classNames('navbar-cog-arrow', {'hidden' : !this.state.settingsOpen})}
            />
            <Dropdown
              isOpen={this.state.settingsOpen}
              toggle={this.toggleSettingsDropdown}
              className="header-actions-dropdown"
            >
              <DropdownMenu>
                {topRow}
                <a
                  target="_blank"
                  href="http://cask.co/community"
                >
                  <div className="dropdown-item">
                    <span className="dropdown-icon fa fa-life-ring"></span>
                    {T.translate('features.Navbar.HeaderActions.support')}
                  </div>
                </a>
                <a
                  target="_blank"
                  href="http://cask.co/"
                >
                  <div className="dropdown-item">
                    <span className="dropdown-icon fa fa-home"></span>
                    {T.translate('features.Navbar.HeaderActions.caskHome')}
                  </div>
                </a>
                <a
                  target="_blank"
                  href="http://docs.cask.co/"
                >
                  <div className="dropdown-item">
                    <span className="dropdown-icon fa fa-file"></span>
                    {T.translate('features.Navbar.HeaderActions.documentation')}
                  </div>
                </a>
                {signoutRow}
              </DropdownMenu>
            </Dropdown>
          </div>
          <div id="header-namespace-dropdown" className="namespace-dropdown">
            <NamespaceDropdown tag={this.props.tag}/>
          </div>
          <div className="products-dropdown">
            <ProductsDrawer currentChoice={this.props.product}/>
          </div>
        </ul>
      </div>
    );
  }
}

HeaderActions.propTypes = {
  tag: PropTypes.string,
  product: PropTypes.string
};
