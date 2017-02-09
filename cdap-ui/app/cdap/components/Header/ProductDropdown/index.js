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

import React, {PropTypes, Component} from 'react';
import NamespaceStore from 'services/NamespaceStore';
import {Dropdown, DropdownToggle, DropdownItem} from 'reactstrap';
import CustomDropdownMenu from 'components/CustomDropdownMenu';
import {Link} from 'react-router';
import RedirectToLogin from 'services/redirect-to-login';
import cookie from 'react-cookie';
import VersionStore from 'services/VersionStore';

import T from 'i18n-react';

require('./ProductDropdown.scss');

export default class ProductDropdown extends Component {
  constructor(props) {
    super(props);
    this.state = {
      toggleDropdown: false,
      username: NamespaceStore.getState().username
    };
  }
  toggleCdapMenuDropdown() {
    this.setState({
      toggleDropdown: !this.state.toggleDropdown
    });
  }
  logout() {
    cookie.remove('show-splash-screen-for-session', {path: '/'});
    RedirectToLogin({statusCode: 401});
  }
  onProfileClick(e) {
    e.nativeEvent.preventDefault();
    e.nativeEvent.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
    return false;
  }
  render() {
    let baseCDAPURL = window.getAbsUIUrl();
    let cdapVersion = VersionStore.getState().version;
    let docsUrl = `http://docs.cask.co/cdap/${cdapVersion}/en/index.html`;
    let managementURL = `${baseCDAPURL}/management`;
    let userSection;
    if (this.state.username && window.CDAP_CONFIG.securityEnabled) {
      userSection = (
        <ul className="dropdown-item user-profile clearfix">
          <DropdownItem
            tag="li"
            header
          >
            <a className="user-section">
              <span className="fa fa-user"></span>
              <span className="user-name">{this.state.username}</span>
            </a>
          </DropdownItem>
          <DropdownItem
            tag="li"
            onClick={this.logout.bind(this)}
          >
            <a>{T.translate('features.Navbar.ProductDropdown.logout')}</a>
          </DropdownItem>
        </ul>
      );
    }

    return (
      <Dropdown
        isOpen={this.state.toggleDropdown}
        className="product-dropdown"
        toggle={this.toggleCdapMenuDropdown.bind(this)}>
        <DropdownToggle caret>
          <div className="cdap-logo"></div>
          <span className="fa fa-caret-down"></span>
        </DropdownToggle>
        <CustomDropdownMenu right>
          <DropdownItem tag="li">
            <a
              target="_blank"
              href="http://cask.co/company/about/"
            >
              {T.translate('features.Navbar.ProductDropdown.aboutLabel')}
            </a>
          </DropdownItem>
          <DropdownItem tag="ul" divider />
          <DropdownItem tag="li">
            {
              !this.props.nativeLink ?
                <Link to={`/management`}>
                  {T.translate('features.Management.Title')}
                </Link>
              :
                <a href={managementURL}>
                  {T.translate('features.Management.Title')}
                </a>
            }
          </DropdownItem>
          <DropdownItem tag="ul" divider />
          <DropdownItem tag="li">
            <a
              target="_blank"
              href="http://cask.co/products/cdap/"
            >
              {T.translate('features.Navbar.ProductDropdown.prodWebsiteLabel')}
            </a>
          </DropdownItem>
          <DropdownItem tag="li">
            <a
              target="_blank"
              href="http://cask.co/community"
            >
              {T.translate('features.Navbar.ProductDropdown.supportLabel')}
            </a>
          </DropdownItem>
          <DropdownItem tag="li">
            <a
              href={docsUrl}
              target="_blank"
            >
              {T.translate('features.Navbar.ProductDropdown.documentationLabel')}
            </a>
          </DropdownItem>
          {
            window.CDAP_CONFIG.securityEnabled ?
              (
                <DropdownItem tag="ul" divider />
              )
            :
              null
          }
          {
            window.CDAP_CONFIG.securityEnabled ?
              userSection
            :
              null
          }
        </CustomDropdownMenu>
      </Dropdown>
    );
  }
}
ProductDropdown.propTypes = {
  nativeLink: PropTypes.bool
};
